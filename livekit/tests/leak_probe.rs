// Copyright 2025 LiveKit, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![cfg(feature = "__lk-e2e-test")]

use anyhow::{anyhow, Context, Result};
use common::audio::{SineParameters, SineTrack};
use livekit::RoomEvent;
use serial_test::serial;
use std::sync::{Arc, Weak};
use tokio::time::{sleep, timeout, Duration};

mod common;

#[test_log::test(tokio::test)]
#[serial]
async fn post_close_weak_probe() -> Result<()> {
    let (room, events) =
        common::test_rooms(1).await?.pop().context("test_rooms returned no room")?;

    sleep(Duration::from_secs(2)).await;

    let room_inner = room.inner_weak();
    let engine_inner = room.engine_inner_weak();
    let rtc_session = room.rtc_session_weak();

    room.close().await.context("room close failed")?;
    drop(events);
    drop(room);

    sleep(Duration::from_secs(3)).await;

    print_weak("bare RoomSession", &room_inner);
    print_weak("bare EngineInner", &engine_inner);
    print_weak("bare RtcSession", &rtc_session);

    Ok(())
}

#[test_log::test(tokio::test)]
#[serial]
async fn local_participant_holder_probe() -> Result<()> {
    let (room, events) =
        common::test_rooms(1).await?.pop().context("test_rooms returned no room")?;
    let local_participant = room.local_participant();

    sleep(Duration::from_secs(2)).await;

    let room_inner = room.inner_weak();
    let engine_inner = room.engine_inner_weak();
    let rtc_session = room.rtc_session_weak();

    room.close().await.context("room close failed")?;
    drop(events);
    drop(room);

    sleep(Duration::from_secs(3)).await;

    print_weak("local_participant_holder RoomSession", &room_inner);
    print_weak("local_participant_holder EngineInner", &engine_inner);
    print_weak("local_participant_holder RtcSession", &rtc_session);

    drop(local_participant);
    sleep(Duration::from_secs(1)).await;

    print_weak("local_participant_released RoomSession", &room_inner);
    print_weak("local_participant_released EngineInner", &engine_inner);
    print_weak("local_participant_released RtcSession", &rtc_session);

    Ok(())
}

#[test_log::test(tokio::test)]
#[serial]
async fn subscriber_track_subscribed_close_does_not_resurrect_publication_cycle() -> Result<()> {
    const ITERATIONS: usize = 10;
    let mut survivors = 0;

    for iteration in 1..=ITERATIONS {
        let mut rooms = common::test_rooms(2).await?;
        let (publisher_room, publisher_events) =
            rooms.pop().context("test_rooms returned no publisher room")?;
        let (subscriber_room, mut subscriber_events) =
            rooms.pop().context("test_rooms returned no subscriber room")?;

        let publisher_room = Arc::new(publisher_room);
        let mut sine_track = SineTrack::new(
            publisher_room.clone(),
            SineParameters { freq: 60.0, amplitude: 1.0, sample_rate: 48_000, num_channels: 1 },
        );
        sine_track.publish().await?;

        timeout(Duration::from_secs(15), wait_for_track_subscribed(&mut subscriber_events))
            .await
            .context("timed out waiting for TrackSubscribed")??;

        let room_inner = subscriber_room.inner_weak();
        let engine_inner = subscriber_room.engine_inner_weak();
        let rtc_session = subscriber_room.rtc_session_weak();

        subscriber_room.close().await.context("subscriber room close failed")?;
        drop(subscriber_events);
        drop(subscriber_room);

        sleep(Duration::from_secs(3)).await;

        let room_alive = weak_alive(&room_inner);
        let engine_alive = weak_alive(&engine_inner);
        let rtc_alive = weak_alive(&rtc_session);
        let iteration_survivors =
            usize::from(room_alive) + usize::from(engine_alive) + usize::from(rtc_alive);
        survivors += iteration_survivors;

        println!(
            "LK_LEAK_PROBE subscriber resurrection iteration {iteration}/{ITERATIONS}: \
             room_alive={room_alive} engine_alive={engine_alive} rtc_alive={rtc_alive} \
             survivor_count={iteration_survivors}"
        );

        sine_track.unpublish().await?;
        publisher_room.close().await.context("publisher room close failed")?;
        drop(sine_track);
        drop(publisher_events);
        drop(publisher_room);
    }

    assert_eq!(survivors, 0, "subscriber-side weak probes survived across {ITERATIONS} iterations");

    Ok(())
}

async fn wait_for_track_subscribed(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
) -> Result<()> {
    loop {
        let Some(event) = events.recv().await else {
            return Err(anyhow!("subscriber event receiver closed before TrackSubscribed"));
        };
        if let RoomEvent::TrackSubscribed { .. } = event {
            return Ok(());
        }
    }
}

fn weak_alive<T>(weak: &Weak<T>) -> bool {
    weak.upgrade().is_some()
}

fn print_weak<T>(label: &str, weak: &Weak<T>) {
    match weak.upgrade() {
        Some(arc) => println!(
            "LK_LEAK_PROBE {label}: alive=true strong_count={} retained_without_probe={}",
            Arc::strong_count(&arc),
            Arc::strong_count(&arc).saturating_sub(1)
        ),
        None => {
            println!("LK_LEAK_PROBE {label}: alive=false strong_count=0 retained_without_probe=0")
        }
    }
}
