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

use anyhow::{Context, Result};
use serial_test::serial;
use std::sync::{Arc, Weak};
use tokio::time::{sleep, Duration};

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
