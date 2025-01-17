use std::{thread, time::Duration};

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::{NakProcedure, PutRequest},
    filestore::FileStore,
    pdu::{
        Condition, EntityID, MessageToUser, ProxyOperation, ProxyPutRequest, TransmissionMode,
        UserOperation,
    },
};

use rstest::{fixture, rstest};

mod common;
use common::{
    get_filestore, new_entities, static_assets, EntityConstructorReturn, StaticAssets,
    TransportIssue, UsersAndFilestore,
};

#[rstest]
#[timeout(Duration::from_secs(200))]
// Series F1
// Sequence 1 Test
// Test goal:
//  - Establish one-way connectivity
// Configuration:
//  - Unacknowledged
//  - File Size: Small (file fits in single pdu)
fn f1s01(get_filestore: &UsersAndFilestore) {
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/small_f1s01.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/small.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F1
// Sequence 2 Test
// Test goal:
//  - Execute Multiple File Data PDUs
// Configuration:
//  - Unacknowledged
//  - File Size: Medium
fn f1s02(get_filestore: &UsersAndFilestore) {
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s02.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    local_user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 3 Test
// Test goal:
//  - Execute Two way communication
// Configuration:
//  - Acknowledged
//  - File Size: Medium
fn f1s03(get_filestore: &UsersAndFilestore) {
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s03.txt".into();
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

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s04(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Rate(13)),
        None,
        [None; 3],
        NakProcedure::Deferred(Duration::ZERO),
    )
}

#[rstest]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 4 Test
// Test goal:
//  - Recovery of Lost data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data lost in transport
fn f1s04(fixture_f1s04: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f1s04;
    let out_file: Utf8PathBuf = "remote/medium_f1s04.txt".into();
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

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s05(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Duplicate(13)),
        None,
        [None; 3],
        NakProcedure::Deferred(Duration::ZERO),
    )
}

#[rstest]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 5 Test
// Test goal:
//  - Ignoring duplicated data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data duplicated in transport
fn f1s05(fixture_f1s05: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f1s05;

    let out_file: Utf8PathBuf = "remote/medium_f1s05.txt".into();
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

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s06(static_assets: &StaticAssets) -> EntityConstructorReturn {
    new_entities(
        static_assets,
        Some(TransportIssue::Reorder(13)),
        None,
        [None; 3],
        NakProcedure::Deferred(Duration::ZERO),
    )
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F1
// Sequence 6 Test
// Test goal:
//  - Reorder Data test
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data re-ordered in transport
fn f1s06(fixture_f1s06: &'static EntityConstructorReturn) {
    // let mut user = User::new(Some(_local_path))
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f1s06;

    let out_file: Utf8PathBuf = "remote/medium_f1s06.txt".into();
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

    assert!(path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 7 Test
// Test goal:
//  - Check User application messaging
// Configuration:
//  - Acknowledged
//  - File Size: Zero
//  - Have proxy put request send Entity 0 data,
//  -   then have a proxy put request in THAT message send data back to entity 1
fn f1s07(get_filestore: &UsersAndFilestore) {
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "/remote/medium_f1s07.txt".into();
    let interim_file: Utf8PathBuf = "/local/medium_f1s07.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);
    let path_interim = filestore.get_native_path(&interim_file);

    local_user
        .put(PutRequest {
            source_filename: "".into(),
            destination_filename: "".into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                        destination_entity_id: EntityID::from(0_u16),
                        source_filename: "/local/medium.txt".into(),
                        destination_filename: interim_file.clone(),
                    }),
                )),
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyTransmissionMode(TransmissionMode::Acknowledged),
                )),
            ],
        })
        .expect("unable to send put request.");
    while !path_interim.exists() {
        thread::sleep(Duration::from_millis(100))
    }
    assert!(path_interim.exists());

    local_user
        .put(PutRequest {
            source_filename: "".into(),
            destination_filename: "".into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                        destination_entity_id: EntityID::from(0_u16),
                        source_filename: "".into(),
                        destination_filename: "".into(),
                    }),
                )),
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyMessageToUser(MessageToUser::from(
                        UserOperation::ProxyOperation(ProxyOperation::ProxyPutRequest(
                            ProxyPutRequest {
                                destination_entity_id: EntityID::from(1_u16),
                                source_filename: interim_file,
                                destination_filename: out_file,
                            },
                        )),
                    )),
                )),
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyMessageToUser(MessageToUser::from(
                        UserOperation::ProxyOperation(ProxyOperation::ProxyTransmissionMode(
                            TransmissionMode::Acknowledged,
                        )),
                    )),
                )),
            ],
        })
        .expect("Unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(100))
    }

    assert!(path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 8 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Sender
fn f1s08(get_filestore: &UsersAndFilestore) {
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s08.txt".into();
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

    while local_user.report(id).expect("cannot send report").is_none() {
        thread::sleep(Duration::from_millis(100));
    }
    local_user.cancel(id).expect("unable to cancel.");

    let mut report = local_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::CancelReceived {
        thread::sleep(Duration::from_millis(100));
        report = local_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::CancelReceived);

    assert!(!path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 9 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Receiver
fn f1s09(get_filestore: &UsersAndFilestore) {
    let (local_user, remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s09.txt".into();
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
    while remote_user
        .report(id)
        .expect("cannot send report")
        .is_none()
    {
        thread::sleep(Duration::from_micros(1));
    }
    remote_user.cancel(id).expect("unable to cancel.");

    let mut report = remote_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::CancelReceived {
        thread::sleep(Duration::from_millis(100));
        report = remote_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::CancelReceived);

    assert!(!path_to_out.exists())
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 10 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Unacknowledged Transmission
//  - Cancel initiated from Sender
fn f1s10(get_filestore: &UsersAndFilestore) {
    let (local_user, _remote_user, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/large_f1s10.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let id = local_user
        .put(PutRequest {
            source_filename: "local/large.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while local_user.report(id).expect("cannot send report").is_none() {
        thread::sleep(Duration::from_millis(100));
    }

    local_user.cancel(id).expect("unable to cancel.");

    let mut report = local_user
        .report(id)
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::CancelReceived {
        thread::sleep(Duration::from_millis(100));
        report = local_user
            .report(id)
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::CancelReceived);

    assert!(!path_to_out.exists())
}
