use std::{
    collections::HashMap,
    net::ToSocketAddrs,
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};

use camino::{Utf8Path, Utf8PathBuf};
use cfdp_core::{
    daemon::PutRequest,
    filestore::{FileStore, NativeFileStore},
    pdu::{
        EntityID, MessageToUser, ProxyOperation, ProxyPutRequest, TransmissionMode, UserOperation,
    },
    transport::{PDUTransport, UdpTransport},
    user::User,
};

use rstest::{fixture, rstest};

mod common;
use common::{create_daemons, get_filestore, tempdir_fixture, JoD, LossyTransport, TransportIssue};
use tempfile::TempDir;

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(2))]
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
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(2))]
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
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(2))]
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s4(
    tempdir_fixture: &TempDir,
    get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>),
) -> (
    String,
    JoD<'static, ()>,
    JoD<'static, ()>,
    Arc<Mutex<NativeFileStore>>,
) {
    let (_, filestore) = get_filestore;
    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(
            EntityID::from(0_u16),
            "127.0.0.1:55347"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp.insert(
            EntityID::from(1_u16),
            "127.0.0.1:55348"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp
    };

    let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(0_u16)],
            Box::new(
                UdpTransport::new("127.0.0.1:55348", entity_map.clone())
                    .expect("Unable to make UdpTransport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(1_u16)],
            Box::new(
                LossyTransport::new("127.0.0.1:55347", entity_map, TransportIssue::Rate(13))
                    .expect("Unable to make Lossy Transport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );
    let (path, local, remote) = create_daemons(
        path.as_path(),
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        "f1s4_local.socket",
        "f1s4_remote.socket",
    );
    (path, local, remote, filestore.clone())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 4 Test
// Test goal:
//  - Recovery of Lost data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data lost in transport
fn f1s4(fixture_f1s4: &'static (String, JoD<()>, JoD<()>, Arc<Mutex<NativeFileStore>>)) {
    // let mut user = User::new(Some(_local_path))
    let (local_path, _local, _remote, filestore) = fixture_f1s4;
    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/medium_f1s4.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s5(
    tempdir_fixture: &TempDir,
    get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>),
) -> (
    String,
    JoD<'static, ()>,
    JoD<'static, ()>,
    Arc<Mutex<NativeFileStore>>,
) {
    let (_, filestore) = get_filestore;
    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(
            EntityID::from(0_u16),
            "127.0.0.1:55349"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp.insert(
            EntityID::from(1_u16),
            "127.0.0.1:55350"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp
    };

    let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(0_u16)],
            Box::new(
                UdpTransport::new("127.0.0.1:55350", entity_map.clone())
                    .expect("Unable to make UdpTransport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(1_u16)],
            Box::new(
                LossyTransport::new("127.0.0.1:55349", entity_map, TransportIssue::Duplicate(13))
                    .expect("Unable to make Lossy Transport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );
    let (path, local, remote) = create_daemons(
        path.as_path(),
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        "f1s5_local.socket",
        "f1s5_remote.socket",
    );
    (path, local, remote, filestore.clone())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 5 Test
// Test goal:
//  - Ignoring duplicated data
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data duplicated in transport
fn f1s5(fixture_f1s5: &'static (String, JoD<()>, JoD<()>, Arc<Mutex<NativeFileStore>>)) {
    // let mut user = User::new(Some(_local_path))
    let (local_path, _local, _remote, filestore) = fixture_f1s5;
    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/medium_f1s5.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists())
}

#[fixture]
#[once]
fn fixture_f1s6(
    tempdir_fixture: &TempDir,
    get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>),
) -> (
    String,
    JoD<'static, ()>,
    JoD<'static, ()>,
    Arc<Mutex<NativeFileStore>>,
) {
    let (_, filestore) = get_filestore;
    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(
            EntityID::from(0_u16),
            "127.0.0.1:55351"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp.insert(
            EntityID::from(1_u16),
            "127.0.0.1:55352"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp
    };

    let remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(0_u16)],
            Box::new(
                UdpTransport::new("127.0.0.1:55352", entity_map.clone())
                    .expect("Unable to make UdpTransport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(1_u16)],
            Box::new(
                LossyTransport::new("127.0.0.1:55351", entity_map, TransportIssue::Duplicate(13))
                    .expect("Unable to make Lossy Transport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );
    let (path, local, remote) = create_daemons(
        path.as_path(),
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        "f1s6_local.socket",
        "f1s6_remote.socket",
    );
    (path, local, remote, filestore.clone())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(30))]
// Series F1
// Sequence 6 Test
// Test goal:
//  - Reorder Data test
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - ~1% data re-ordered in transport
fn f1s6(fixture_f1s6: &'static (String, JoD<()>, JoD<()>, Arc<Mutex<NativeFileStore>>)) {
    // let mut user = User::new(Some(_local_path))
    let (local_path, _local, _remote, filestore) = fixture_f1s6;
    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/medium_f1s6.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
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
fn f1s7(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "/remote/medium_f1s7.txt".into();
    let interim_file: Utf8PathBuf = "/local/medium_f1s7.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);
    let path_interim = filestore.lock().unwrap().get_native_path(&interim_file);

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
    .expect("unable to send put request.");
    while !path_interim.exists() {
        thread::sleep(Duration::from_millis(1))
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 7 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Sender
fn f1s8(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/medium_f1s8.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);

    let id = user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    thread::sleep(Duration::from_millis(2));

    user.cancel(id.clone())
        .expect("Unable to Cancel transaction.");
    thread::sleep(Duration::from_millis(1));
    user.report(id).expect("Unable to send Report Request.");
    thread::sleep(Duration::from_millis(3));

    assert!(!path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 7 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Sender
fn f1s9(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let mut user_remote = User::new(Some(
        Utf8Path::new(local_path)
            .parent()
            .unwrap()
            .join("cfdp_remote.socket")
            .as_str(),
    ))
    .expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/medium_f1s9.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);

    let id = user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    thread::sleep(Duration::from_millis(2));

    user_remote
        .cancel(id.clone())
        .expect("Unable to Cancel transaction.");
    thread::sleep(Duration::from_millis(25));
    user.report(id).expect("Unable to send Report Request.");
    thread::sleep(Duration::from_millis(50));

    assert!(!path_to_out.exists())
}

#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
// Series F1
// Sequence 7 Test
// Test goal:
//  - Check User Cancel Functionality
// Configuration:
//  - Cancel initiated from Sender
fn f1s10(get_filestore: &(&'static String, Arc<Mutex<NativeFileStore>>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");
    let mut user_remote = User::new(Some(
        Utf8Path::new(local_path)
            .parent()
            .unwrap()
            .join("cfdp_remote.socket")
            .as_str(),
    ))
    .expect("User Cannot connect to Daemon.");
    let out_file: Utf8PathBuf = "remote/medium_f1s10.txt".into();
    let path_to_out = filestore.lock().unwrap().get_native_path(&out_file);

    let id = user
        .put(PutRequest {
            source_filename: "local/medium.txt".into(),
            destination_filename: out_file,
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Unacknowledged,
            filestore_requests: vec![],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    thread::sleep(Duration::from_millis(2));

    user_remote
        .cancel(id.clone())
        .expect("Unable to Cancel transaction.");
    thread::sleep(Duration::from_millis(25));
    user.report(id).expect("Unable to send Report Request.");
    thread::sleep(Duration::from_millis(50));

    assert!(!path_to_out.exists())
}
