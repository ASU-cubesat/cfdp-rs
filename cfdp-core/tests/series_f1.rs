use std::{
    collections::HashMap,
    // net::UdpSocket,
    sync::{atomic::AtomicBool, Arc},
};

use camino::{Utf8Path, Utf8PathBuf};
use cfdp_core::{
    daemon::PutRequest,
    filestore::{FileStore, NativeFileStore},
    pdu::{
        Condition, EntityID, MessageToUser, ProxyOperation, ProxyPutRequest, TransmissionMode,
        UserOperation,
    },
    transport::{PDUTransport, UdpTransport},
    user::User,
};

use rstest::{fixture, rstest};
use tokio::{net::UdpSocket, time::Duration};

mod common;

use common::{
    create_daemons, get_filestore, tempdir_fixture, terminate, EntityConstructorReturn,
    LossyTransport, TransportIssue,
};
use tempfile::TempDir;

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(2))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 1 Test
// Test goal:
//  - Establish one-way connectivity
// Configuration:
//  - Unacknowledged
//  - File Size: Small (file fits in single pdu)
async fn f1s01(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;
    let out_file: Utf8PathBuf = "remote/small_f1s1.txt".into();

    let task_outfile = out_file.clone();

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");

    user.put(PutRequest {
        source_filename: "local/small.txt".into(),
        destination_filename: task_outfile,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Unacknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .await
    .expect("unable to send put request.");

    let path_to_out = filestore.get_native_path(&out_file);

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(15))]
#[tokio::test(flavor = "multi_thread")]
// // Series F1
// // Sequence 2 Test
// // Test goal:
// //  - Execute Multiple File Data PDUs
// // Configuration:
// //  - Unacknowledged
// //  - File Size: Medium
async fn f1s02(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s2.txt".into();

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");
    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: out_file.clone(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Unacknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .await
    .expect("unable to send put request.");

    let path_to_out = filestore.get_native_path(&out_file);

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 3 Test
// Test goal:
//  - Execute Two way communication
// Configuration:
//  - Acknowledged
//  - File Size: Medium
async fn f1s03(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s3.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let task_outfile = out_file.clone();

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");
    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: task_outfile,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .await
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s04(
    tempdir_fixture: &TempDir,
    get_filestore: &(&'static String, Arc<NativeFileStore>),
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, filestore) = get_filestore;

    let utf8_path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );
    let local_path = utf8_path.join("f1s4_local.socket").as_str().to_owned();

    let (local, remote) = block_on!(async {
        let remote_udp = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Unable to bind remote UDP.");
        let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

        let local_udp = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Unable to bind local UDP.");
        let local_addr = local_udp.local_addr().expect("Cannot find local address.");

        let entity_map = {
            let mut temp = HashMap::new();
            temp.insert(EntityID::from(0_u16), local_addr);
            temp.insert(EntityID::from(1_u16), remote_addr);
            temp
        };
        let local_transport =
            LossyTransport::try_from((local_udp, entity_map.clone(), TransportIssue::Rate(50)))
                .expect("Unable to make Lossy Transport.");
        let remote_transport =
            UdpTransport::try_from((remote_udp, entity_map)).expect("Unable to make UdpTransport.");

        let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
            HashMap::from([(
                vec![EntityID::from(0_u16)],
                Box::new(remote_transport) as Box<dyn PDUTransport + Send>,
            )]);

        let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
            HashMap::from([(
                vec![EntityID::from(1_u16)],
                Box::new(local_transport) as Box<dyn PDUTransport + Send>,
            )]);

        create_daemons(
            utf8_path.as_path(),
            filestore.clone(),
            local_transport_map,
            remote_transport_map,
            "f1s4_local.socket",
            "f1s4_remote.socket",
            terminate.clone(),
            [None; 3],
        )
        .await
    });
    (local_path, filestore.clone(), local, remote)
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(15))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 4 Test
// Test goal:
//  - Recovery of Lost data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data lost in transport
async fn f1s04(fixture_f1s04: &'static EntityConstructorReturn) {
    let (local_path, filestore, _local, _remote) = fixture_f1s04;
    let out_file: Utf8PathBuf = "remote/medium_f1s4.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");

    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: out_file.clone(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .await
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s05(
    tempdir_fixture: &TempDir,
    get_filestore: &(&'static String, Arc<NativeFileStore>),
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, filestore) = get_filestore;

    let utf8_path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );

    let local_path = utf8_path.join("f1s5_local.socket").as_str().to_owned();
    let inner_path = utf8_path;

    let inner_fs = filestore.clone();
    let inner_term = terminate.clone();
    let (local, remote) = block_on!(async {
        let remote_udp = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Unable to bind remote UDP.");
        let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

        let local_udp = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Unable to bind local UDP.");
        let local_addr = local_udp.local_addr().expect("Cannot find local address.");

        let entity_map = {
            let mut temp = HashMap::new();
            temp.insert(EntityID::from(0_u16), local_addr);
            temp.insert(EntityID::from(1_u16), remote_addr);
            temp
        };
        let local_transport = LossyTransport::try_from((
            local_udp,
            entity_map.clone(),
            TransportIssue::Duplicate(13),
        ))
        .expect("Unable to make Lossy Transport.");
        let remote_transport =
            UdpTransport::try_from((remote_udp, entity_map)).expect("Unable to make UdpTransport.");

        let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
            HashMap::from([(
                vec![EntityID::from(0_u16)],
                Box::new(remote_transport) as Box<dyn PDUTransport + Send>,
            )]);

        let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
            HashMap::from([(
                vec![EntityID::from(1_u16)],
                Box::new(local_transport) as Box<dyn PDUTransport + Send>,
            )]);

        create_daemons(
            inner_path.as_path(),
            inner_fs,
            local_transport_map,
            remote_transport_map,
            "f1s5_local.socket",
            "f1s5_remote.socket",
            inner_term,
            [None; 3],
        )
        .await
    });
    (local_path, filestore.clone(), local, remote)
}

#[rstest]
#[timeout(Duration::from_secs(30))]
#[cfg_attr(target_os = "windows", ignore)]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 5 Test
// Test goal:
//  - Ignoring duplicated data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data duplicated in transport
async fn f1s05(fixture_f1s05: &'static EntityConstructorReturn) {
    let (local_path, filestore, _local, _remote) = fixture_f1s05;

    let out_file: Utf8PathBuf = "remote/medium_f1s5.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");

    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: out_file,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .await
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s06(
    tempdir_fixture: &TempDir,
    get_filestore: &(&'static String, Arc<NativeFileStore>),
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, filestore) = get_filestore;

    let utf8_path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );

    let local_path = utf8_path.join("f1s6_local.socket").as_str().to_owned();

    let (local, remote) = block_on!(async {
        let remote_udp = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Unable to bind remote UDP.");
        let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

        let local_udp = UdpSocket::bind("127.0.0.1:0")
            .await
            .expect("Unable to bind local UDP.");
        let local_addr = local_udp.local_addr().expect("Cannot find local address.");

        let entity_map = {
            let mut temp = HashMap::new();
            temp.insert(EntityID::from(0_u16), local_addr);
            temp.insert(EntityID::from(1_u16), remote_addr);
            temp
        };
        let local_transport =
            LossyTransport::try_from((local_udp, entity_map.clone(), TransportIssue::Reorder(13)))
                .expect("Unable to make Lossy Transport.");
        let remote_transport =
            UdpTransport::try_from((remote_udp, entity_map)).expect("Unable to make UdpTransport.");

        let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
            HashMap::from([(
                vec![EntityID::from(0_u16)],
                Box::new(remote_transport) as Box<dyn PDUTransport + Send>,
            )]);

        let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
            HashMap::from([(
                vec![EntityID::from(1_u16)],
                Box::new(local_transport) as Box<dyn PDUTransport + Send>,
            )]);

        create_daemons(
            utf8_path.as_path(),
            filestore.clone(),
            local_transport_map,
            remote_transport_map,
            "f1s6_local.socket",
            "f1s6_remote.socket",
            terminate.clone(),
            [None; 3],
        )
        .await
    });
    (local_path, filestore.clone(), local, remote)
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 6 Test
// Test goal:
//  - Reorder Data test
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data re-ordered in transport
async fn f1s06(fixture_f1s06: &'static EntityConstructorReturn) {
    let (local_path, filestore, _local, _remote) = fixture_f1s06;

    let out_file: Utf8PathBuf = "remote/medium_f1s6.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");
    user.put(PutRequest {
        source_filename: "local/medium.txt".into(),
        destination_filename: out_file,
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .await
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(path_to_out.exists())
}

#[rstest]
#[ignore]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(30))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 7 Test
// Test goal:
//  - Check User application messaging
// Configuration:
//  - Acknowledged
//  - File Size: Zero
//  - Have proxy put request send Entity 0 data,
//  -   then have a proxy put request in THAT message send data back to entity 1
async fn f1s07(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "/remote/medium_f1s7.txt".into();
    let interim_file: Utf8PathBuf = "/local/medium_f1s7.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);
    let path_interim = filestore.get_native_path(&interim_file);
    println!("{path_to_out:?}");

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");

    user.put(PutRequest {
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
    .await
    .expect("unable to send put request.");

    while !path_interim.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(path_interim.exists());

    user.put(PutRequest {
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
                            source_filename: interim_file.clone(),
                            destination_filename: out_file.clone(),
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
    .await
    .expect("Unable to send second put request");

    while !path_to_out.exists() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 8 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Sender
async fn f1s08(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s8.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let local_path = (*local_path).clone();

    let mut user = User::new(Some(&local_path))
        .await
        .expect("User Cannot connect to Daemon.");
    let id = user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .await
        .expect("unable to send put request.");
    while user.report(id).await.expect("cannot send report").is_none() {
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    user.cancel(id).await.expect("unable to cancel.");
    tokio::time::sleep(Duration::from_millis(5)).await;

    let mut report = user
        .report(id)
        .await
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::CancelReceived {
        tokio::time::sleep(Duration::from_millis(5)).await;
        report = user
            .report(id)
            .await
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::CancelReceived);

    assert!(!path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(30))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 9 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Receiver
async fn f1s09(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let out_file: Utf8PathBuf = "remote/medium_f1s9.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");

    let mut user_remote = User::new(Some(
        Utf8Path::new(local_path)
            .parent()
            .unwrap()
            .join("cfdp_remote.socket")
            .as_str(),
    ))
    .await
    .expect("User Cannot connect to Daemon.");

    let id = user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .await
        .expect("unable to send put request.");

    // the transaction is actually going too quickly
    // and the file is being finalized
    // user.suspend(id).await.expect("unable to cancel.");
    while user_remote
        .report(id)
        .await
        .expect("cannot send report")
        .is_none()
    {
        tokio::time::sleep(Duration::from_micros(1)).await;
    }

    user_remote.cancel(id).await.expect("unable to cancel.");

    let mut report = user_remote
        .report(id)
        .await
        .expect("Unable to send Report Request.")
        .unwrap();

    while report.condition != Condition::CancelReceived {
        tokio::time::sleep(Duration::from_millis(5)).await;
        report = user_remote
            .report(id)
            .await
            .expect("Unable to send Report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::CancelReceived);
    println!("{path_to_out:?}");
    assert!(!path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
#[tokio::test(flavor = "multi_thread")]
// Series F1
// Sequence 10 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Unacknowledged Transmission
//  - Cancel initiated from Sender
async fn f1s10(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path))
        .await
        .expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/large_f1s10.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    let id = user
        .put(PutRequest {
            source_filename: "local/large.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .await
        .expect("unable to send put request.");

    while user.report(id).await.expect("cannot send report").is_none() {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    tokio::time::sleep(Duration::from_millis(10)).await;
    user.cancel(id).await.expect("unable to cancel.");
    tokio::time::sleep(Duration::from_millis(10)).await;

    let mut report = user
        .report(id)
        .await
        .expect("Unable to send later report Request.")
        .unwrap();

    while report.condition != Condition::CancelReceived {
        tokio::time::sleep(Duration::from_millis(10)).await;
        report = user
            .report(id)
            .await
            .expect("Unable to send last report Request.")
            .unwrap();
    }

    assert_eq!(report.condition, Condition::CancelReceived);

    assert!(!path_to_out.exists())
}
