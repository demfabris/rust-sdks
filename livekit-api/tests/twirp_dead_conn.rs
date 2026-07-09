// Reproduces the production origination-blackhole signature: the twirp
// client multiplexes every request onto one pooled HTTP/2 connection, and
// when that connection's network path dies *silently* (no RST, no GOAWAY —
// an LB node going dark, a NAT entry vanishing), requests hang with no
// error for as long as kernel TCP retransmission keeps the socket alive.
//
// The proxy below emulates the silent death: at `cut()` it stops forwarding
// bytes on existing flows but keeps their sockets open, while connections
// opened after the cut forward normally (the healthy-path-exists scenario:
// a reconnect would succeed — if the client ever reconnects).
//
// Test 1 shows the failure: without h2 keep-alive the client never abandons
// the dead connection; requests hang indefinitely, back to back.
// Test 2 shows the fix: keep-alive PINGs detect the dead connection within
// interval + timeout, evict it, and the next request succeeds on a fresh
// connection.
#![cfg(feature = "services-tokio")]

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use http::header::HeaderMap;
use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::Response;
use hyper_util::rt::{TokioExecutor, TokioIo};
use livekit_api::services::twirp_client::TwirpClient;
use livekit_protocol::{ListRoomsRequest, ListRoomsResponse};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Minimal h2c twirp server: 200 + empty protobuf body for every POST,
/// which decodes to `Default` for any response message.
async fn spawn_twirp_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { break };
            tokio::spawn(async move {
                let service = service_fn(|_req| async {
                    Ok::<_, hyper::Error>(
                        Response::builder()
                            .status(200)
                            .header("content-type", "application/protobuf")
                            .body(Full::new(Bytes::new()))
                            .unwrap(),
                    )
                });
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    addr
}

#[derive(Clone)]
struct BlackholeProxy {
    addr: SocketAddr,
    /// Generation assigned to each accepted connection.
    generation: Arc<AtomicU64>,
    /// Connections with generation < cut are silently blackholed.
    cut: Arc<AtomicU64>,
    /// Total connections accepted (for asserting reconnect behavior).
    accepted: Arc<AtomicU64>,
}

impl BlackholeProxy {
    async fn spawn(upstream: SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = Self {
            addr: listener.local_addr().unwrap(),
            generation: Arc::new(AtomicU64::new(0)),
            cut: Arc::new(AtomicU64::new(0)),
            accepted: Arc::new(AtomicU64::new(0)),
        };
        let accept = proxy.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else { break };
                let my_gen = accept.generation.fetch_add(1, Ordering::SeqCst);
                accept.accepted.fetch_add(1, Ordering::SeqCst);
                let cut = accept.cut.clone();
                tokio::spawn(async move {
                    let Ok(server) = TcpStream::connect(upstream).await else { return };
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    tokio::join!(
                        pipe(cr, sw, my_gen, cut.clone()),
                        pipe(sr, cw, my_gen, cut),
                    );
                });
            }
        });
        proxy
    }

    /// Silently kill every currently-open flow. Sockets stay open; bytes
    /// stop moving. Connections opened after this forward normally.
    fn cut(&self) {
        self.cut.store(self.generation.load(Ordering::SeqCst), Ordering::SeqCst);
    }

    fn connections_seen(&self) -> u64 {
        self.accepted.load(Ordering::SeqCst)
    }
}

async fn pipe(
    mut src: tokio::net::tcp::OwnedReadHalf,
    mut dst: tokio::net::tcp::OwnedWriteHalf,
    my_gen: u64,
    cut: Arc<AtomicU64>,
) {
    let mut buf = [0u8; 16 * 1024];
    loop {
        let Ok(n) = src.read(&mut buf).await else { return };
        if n == 0 {
            return;
        }
        if my_gen < cut.load(Ordering::SeqCst) {
            // Silent death: swallow traffic forever without closing either
            // socket, exactly like a path that stopped delivering packets.
            std::future::pending::<()>().await;
        }
        if dst.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

async fn list_rooms(twirp: &TwirpClient) -> Result<ListRoomsResponse, impl std::fmt::Debug> {
    twirp
        .request::<_, ListRoomsResponse>(
            "RoomService",
            "ListRooms",
            ListRoomsRequest::default(),
            HeaderMap::new(),
        )
        .await
}

#[tokio::test]
async fn dead_connection_without_keepalive_hangs_indefinitely() {
    let server = spawn_twirp_server().await;
    let proxy = BlackholeProxy::spawn(server).await;

    // The pre-fix client shape: pooled h2, no keep-alive, no timeouts.
    let client = reqwest::Client::builder().http2_prior_knowledge().build().unwrap();
    let twirp = TwirpClient::with_client(client, &format!("http://{}", proxy.addr), "livekit", None);

    list_rooms(&twirp).await.expect("healthy path should serve request 1");
    proxy.cut();

    let hung = tokio::time::timeout(Duration::from_secs(3), list_rooms(&twirp)).await;
    assert!(hung.is_err(), "request on a silently-dead connection must hang, got {hung:?}");

    // The corpse is still pooled: later requests keep multiplexing onto it.
    let still_hung = tokio::time::timeout(Duration::from_secs(3), list_rooms(&twirp)).await;
    assert!(still_hung.is_err(), "connection is never evicted without keep-alive, got {still_hung:?}");

    assert_eq!(
        proxy.connections_seen(),
        1,
        "client must never have attempted a fresh connection"
    );
}

#[tokio::test]
async fn keepalive_evicts_dead_connection_and_recovers() {
    let server = spawn_twirp_server().await;
    let proxy = BlackholeProxy::spawn(server).await;

    // The fix shape (short intervals to keep the test fast; production uses
    // 10s/5s via http_client::new_client through TwirpClient::new).
    let client = reqwest::Client::builder()
        .http2_prior_knowledge()
        .http2_keep_alive_interval(Duration::from_secs(1))
        .http2_keep_alive_timeout(Duration::from_secs(1))
        .http2_keep_alive_while_idle(true)
        .build()
        .unwrap();
    let twirp = TwirpClient::with_client(client, &format!("http://{}", proxy.addr), "livekit", None);

    list_rooms(&twirp).await.expect("healthy path should serve request 1");
    proxy.cut();

    // In-flight/next request may fail fast or get retried on a fresh
    // connection; either way it must not hang past ping detection.
    let _ = tokio::time::timeout(Duration::from_secs(5), list_rooms(&twirp)).await;

    // Give the keep-alive probe one full cycle to evict the corpse.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let recovered = tokio::time::timeout(Duration::from_secs(5), list_rooms(&twirp))
        .await
        .expect("request after eviction must not hang");
    recovered.expect("request after eviction must succeed on a fresh connection");

    assert!(
        proxy.connections_seen() >= 2,
        "recovery requires a fresh connection, saw {}",
        proxy.connections_seen()
    );
}
