use std::{thread, time::Duration};

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::PutRequest,
    filestore::FileStore,
    pdu::{Condition, EntityID, PDUDirective, TransmissionMode},
};
use rstest::{fixture, rstest};

mod common;
use common::{new_entities, static_assets, EntityConstructorReturn, StaticAssets, TransportIssue};

#[fixture]
#[once]
fn fixture_f2s01(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Once(PDUDirective::Metadata)),
        None,
        [None; 3],
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 1 Test
// Test goal:
//  - Recover from Loss of Metadata PDU
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of Metadata PDU
fn f2s01(fixture_f2s01: &'static EntityConstructorReturn) {
    // let mut user = User::new(Some(_local_path))
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s01;

    let out_file: Utf8PathBuf = "remote/medium_f2s01.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s02(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Once(PDUDirective::EoF)),
        None,
        [None; 3],
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 2 Test
// Test goal:
//  - Recover from Loss of EoF PDU
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of EoF PDU
fn f2s02(fixture_f2s02: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s02;

    let out_file: Utf8PathBuf = "remote/medium_f2s02.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s03(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Once(PDUDirective::Finished)),
        None,
        [None; 3],
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 3 Test
// Test goal:
//  - Recover from Loss of Finished PDU
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of Finished PDU
fn f2s03(fixture_f2s03: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s03;

    let out_file: Utf8PathBuf = "remote/medium_f2s03.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s04(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        Some(TransportIssue::Once(PDUDirective::Ack)),
        [None; 3],
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 3 Test
// Test goal:
//  - Recover from Loss of ACK(EOF) PDU
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of ACK(EOF) PDU
fn f2s04(fixture_f2s04: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s04;

    let out_file: Utf8PathBuf = "remote/medium_f2s04.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s05(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Once(PDUDirective::Ack)),
        None,
        [None; 3],
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 5 Test
// Test goal:
//  - Recover from Loss of ACK(Fin) PDU
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of ACK(Fin) PDU
fn f2s05(fixture_f2s05: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s05;

    let out_file: Utf8PathBuf = "remote/medium_f2s05.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s06(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Every),
        Some(TransportIssue::Every),
        [None; 3],
    )
}

#[rstest]
#[timeout(Duration::from_secs(15))]
// Series F2
// Sequence 6 Test
// Test goal:
//  - Recover from noisy environment
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of Every non-EOF pdu in both directions
fn f2s06(fixture_f2s06: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s06;

    let out_file: Utf8PathBuf = "remote/medium_f2s06.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s07(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        Some(TransportIssue::All(vec![
            PDUDirective::Finished,
            PDUDirective::Ack,
        ])),
        [Some(10), None, None],
    )
}

#[rstest]
#[timeout(Duration::from_secs(15))]
// Series F2
// Sequence 7 Test
// Test goal:
//  - check ACK limit reached at Sender
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop all ACK and Finished PDUs
fn f2s07(fixture_f2s07: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s07;

    let out_file: Utf8PathBuf = "remote/medium_f2s07.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let id = local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }
    assert!(path_to_out.exists());
    // wait long enough for the ack limit to be reached

    let mut report = local_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::PositiveLimitReached {
        thread::sleep(Duration::from_millis(100));
        report = local_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::PositiveLimitReached)
}

#[fixture]
#[once]
fn fixture_f2s08(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::All(vec![PDUDirective::Metadata])),
        Some(TransportIssue::All(vec![PDUDirective::Nak])),
        [Some(10), Some(1), Some(1)],
    )
}

#[rstest]
#[timeout(Duration::from_secs(15))]
// Series F2
// Sequence 8 Test
// Test goal:
//  - check NAK limit reached at Receiver
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop all NAK from receiver.
fn f2s08(fixture_f2s08: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s08;

    let out_file: Utf8PathBuf = "remote/medium_f2s08.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let id = local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    // wait long enough for the nak limit to be reached
    let mut report = local_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::NakLimitReached {
        thread::sleep(Duration::from_millis(100));
        report = local_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert!(!path_to_out.exists());

    assert_eq!(report.condition, Condition::NakLimitReached)
}

#[fixture]
#[once]
fn fixture_f2s09(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        None,
        Some(TransportIssue::All(vec![PDUDirective::Finished])),
        [Some(1), Some(10), Some(1)],
    )
}

#[rstest]
#[timeout(Duration::from_secs(15))]
// Series F2
// Sequence 9 Test
// Test goal:
//  - check Inactivity at sender
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop all Finished from receiver.
fn f2s09(fixture_f2s09: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s09;

    let out_file: Utf8PathBuf = "remote/medium_f2s09.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let id = local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    // wait long enough for the nak limit to be reached
    let mut report = local_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::InactivityDetected {
        thread::sleep(Duration::from_millis(100));
        report = local_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    // file is still successfully sent
    assert!(path_to_out.exists());

    assert_eq!(report.condition, Condition::InactivityDetected)
}

#[fixture]
#[once]
fn fixture_f2s10(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Inactivity),
        None,
        [Some(1), Some(10), Some(10)],
    )
}

#[rstest]
#[timeout(Duration::from_secs(15))]
// Series F2
// Sequence 10 Test
// Test goal:
//  - check Inactivity at Receiver
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop every PDU but the first from the sender
fn f2s10(fixture_f2s10: &'static EntityConstructorReturn) {
    let (local_user, remote_user, filestore, _local, _remote) = fixture_f2s10;

    let out_file: Utf8PathBuf = "remote/medium_f2s10.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let id = local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    // wait long enough for the nak limit to be reached
    while remote_user
        .report(id)
        .expect("Unable to send Report Request.")
        .is_none()
    {
        thread::sleep(Duration::from_millis(100))
    }

    let mut report = remote_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::InactivityDetected {
        thread::sleep(Duration::from_millis(100));
        report = remote_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    // file is still successfully sent
    assert!(!path_to_out.exists());

    assert_eq!(report.condition, Condition::InactivityDetected)
}
