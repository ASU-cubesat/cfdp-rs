use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::PutRequest,
    filestore::{FileStore, NativeFileStore},
    pdu::{EntityID, TransmissionMode},
    user::User,
};

use rstest::rstest;

mod common;
use common::get_filestore;

#[rstest]
// Series F1
// Sequence 1 Test
// Test goal:
//  - Establish one-way connectivity
// Configuration:
//  - Unacknowledged
//  - File Size: Small (file fits in single pdu)
fn f1s1(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/small_f1s1.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "local/small.txt".into(),
        destination_filename: out_file,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Unacknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(50))
    }
    assert!(path_to_out.exists())
}

#[rstest]
// Series F1
// Sequence 2 Test
// Test goal:
//  - Execute Multiple File Data PDUs
// Configuration:
//  - Unacknowledged
//  - File Size: Medium
fn f1s2(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/medium_f1s2.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: out_file,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Unacknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(50))
    }
    assert!(path_to_out.exists())
}

#[rstest]
// Series F1
// Sequence 3 Test
// Test goal:
//  - Execute Two way communication
// Configuration:
//  - Acknowledged
//  - File Size: Medium
fn f1s3(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/medium_f1s3.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: out_file,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(50))
    }

    assert!(path_to_out.exists())
}
