use std::{
    collections::HashMap,
    net::UdpSocket,
    sync::{atomic::AtomicBool, Arc},
    thread,
    time::Duration,
};

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::PutRequest,
    filestore::FileStore,
    pdu::{Condition, EntityID, PDUDirective, TransmissionMode},
    transport::{PDUTransport, UdpTransport},
};
use rstest::{fixture, rstest};

mod common;
use common::{
    create_daemons, get_filestore, terminate, EntityConstructorReturn, LossyTransport,
    TransportIssue, UsersAndFilestore,
};

#[fixture]
#[once]
fn fixture_f2s1(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;

    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
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
        TransportIssue::Once(PDUDirective::Metadata),
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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None; 3],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
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
fn f2s1(fixture_f2s1: &'static EntityConstructorReturn) {
    // let mut user = User::new(Some(_local_path))
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s1;

    let out_file: Utf8PathBuf = "remote/medium_f2s1.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s2(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;

    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
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
        TransportIssue::Once(PDUDirective::EoF),
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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None; 3],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
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
fn f2s2(fixture_f2s2: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s2;

    let out_file: Utf8PathBuf = "remote/medium_f2s2.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s3(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(EntityID::from(0_u16), local_addr);
        temp.insert(EntityID::from(1_u16), remote_addr);
        temp
    };

    let local_transport = UdpTransport::try_from((local_udp, entity_map.clone()))
        .expect("Unable to make UdpTransport.");
    let remote_transport = LossyTransport::try_from((
        remote_udp,
        entity_map,
        TransportIssue::Once(PDUDirective::Finished),
    ))
    .expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None; 3],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
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
fn f2s3(fixture_f2s3: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s3;

    let out_file: Utf8PathBuf = "remote/medium_f2s3.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s4(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(EntityID::from(0_u16), local_addr);
        temp.insert(EntityID::from(1_u16), remote_addr);
        temp
    };

    let local_transport = UdpTransport::try_from((local_udp, entity_map.clone()))
        .expect("Unable to make UdpTransport.");
    let remote_transport = LossyTransport::try_from((
        remote_udp,
        entity_map,
        TransportIssue::Once(PDUDirective::Ack),
    ))
    .expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None; 3],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
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
fn f2s4(fixture_f2s4: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s4;

    let out_file: Utf8PathBuf = "remote/medium_f2s4.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s5(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
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
        TransportIssue::Once(PDUDirective::Ack),
    ))
    .expect("Unable to make UdpTransport.");
    let remote_transport =
        UdpTransport::try_from((remote_udp, entity_map)).expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None; 3],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
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
fn f2s5(fixture_f2s5: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s5;

    let out_file: Utf8PathBuf = "remote/medium_f2s5.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s6(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(EntityID::from(0_u16), local_addr);
        temp.insert(EntityID::from(1_u16), remote_addr);
        temp
    };

    let local_transport =
        LossyTransport::try_from((local_udp, entity_map.clone(), TransportIssue::Every))
            .expect("Unable to make UdpTransport.");
    let remote_transport =
        LossyTransport::try_from((remote_udp, entity_map, TransportIssue::Every))
            .expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None; 3],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F2
// Sequence 6 Test
// Test goal:
//  - Recover from noisy environment
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop first instance of Every non-EOF pdu in both directions
fn f2s6(fixture_f2s6: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s6;

    let out_file: Utf8PathBuf = "remote/medium_f2s6.txt".into();
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
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
}

#[fixture]
#[once]
fn fixture_f2s7(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(EntityID::from(0_u16), local_addr);
        temp.insert(EntityID::from(1_u16), remote_addr);
        temp
    };

    let local_transport = UdpTransport::try_from((local_udp, entity_map.clone()))
        .expect("Unable to make UdpTransport.");
    let remote_transport = LossyTransport::try_from((
        remote_udp,
        entity_map,
        TransportIssue::All(vec![PDUDirective::Finished, PDUDirective::Ack]),
    ))
    .expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [Some(10), None, None],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
}

#[rstest]
#[timeout(Duration::from_secs(10))]
// Series F2
// Sequence 7 Test
// Test goal:
//  - check ACK limit reached at Sender
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop all ACK and Finished PDUs
fn f2s7(fixture_f2s7: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s7;

    let out_file: Utf8PathBuf = "remote/medium_f2s7.txt".into();
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
        thread::sleep(Duration::from_millis(1))
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
fn fixture_f2s8(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
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
        TransportIssue::All(vec![PDUDirective::Metadata]),
    ))
    .expect("Unable to Lossy Transport.");
    let remote_transport = LossyTransport::try_from((
        remote_udp,
        entity_map,
        TransportIssue::All(vec![PDUDirective::Nak]),
    ))
    .expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [Some(10), Some(1), Some(1)],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 8 Test
// Test goal:
//  - check NAK limit reached at Receiver
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop all NAK from receiver.
fn f2s8(fixture_f2s8: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s8;

    let out_file: Utf8PathBuf = "remote/medium_f2s8.txt".into();
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
fn fixture_f2s9(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(EntityID::from(0_u16), local_addr);
        temp.insert(EntityID::from(1_u16), remote_addr);
        temp
    };

    let local_transport = UdpTransport::try_from((local_udp, entity_map.clone()))
        .expect("Unable to Lossy Transport.");
    let remote_transport = LossyTransport::try_from((
        remote_udp,
        entity_map,
        TransportIssue::All(vec![PDUDirective::Finished]),
    ))
    .expect("Unable to make Lossy Transport.");

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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [Some(1), Some(10), Some(1)],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
}

#[rstest]
#[timeout(Duration::from_secs(5))]
// Series F2
// Sequence 9 Test
// Test goal:
//  - check Inactivity at sender
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Drop all Finished from receiver.
fn f2s9(fixture_f2s9: &'static EntityConstructorReturn) {
    let (local_user, _remote_user, filestore, _local, _remote) = fixture_f2s9;

    let out_file: Utf8PathBuf = "remote/medium_f2s9.txt".into();
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
fn fixture_f2s10(
    get_filestore: &UsersAndFilestore,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let (_, _, filestore) = get_filestore;
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(EntityID::from(0_u16), local_addr);
        temp.insert(EntityID::from(1_u16), remote_addr);
        temp
    };

    let local_transport =
        LossyTransport::try_from((local_udp, entity_map.clone(), TransportIssue::Inactivity))
            .expect("Unable to Lossy Transport.");
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

    let (local_user, remote_user, local, remote) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [Some(1), Some(10), Some(10)],
    );
    (local_user, remote_user, filestore.clone(), local, remote)
}

#[rstest]
#[timeout(Duration::from_secs(5))]
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
        thread::sleep(Duration::from_millis(5))
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
