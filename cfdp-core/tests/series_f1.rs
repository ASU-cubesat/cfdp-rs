use std::{
    collections::HashMap,
    fs,
    net::ToSocketAddrs,
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::{Daemon, EntityConfig, PutRequest},
    filestore::{ChecksumType, FileStore, NativeFileStore},
    pdu::{CRCFlag, EntityID, TransactionSeqNum, TransmissionMode},
    transport::{PDUTransport, UdpTransport},
    user::User,
};
use signal_hook::{consts::TERM_SIGNALS, flag};
use tempfile::TempDir;

use rstest::{fixture, rstest};

struct JoD<T> {
    handle: Vec<JoinHandle<T>>,
    signal: Arc<AtomicBool>,
}
impl<T> From<(JoinHandle<T>, Arc<AtomicBool>)> for JoD<T> {
    fn from(input: (JoinHandle<T>, Arc<AtomicBool>)) -> Self {
        Self {
            handle: vec![input.0],
            signal: input.1,
        }
    }
}

impl<T> Drop for JoD<T> {
    fn drop(&mut self) {
        self.signal.store(true, Ordering::Relaxed);
        let handle = self.handle.remove(0);
        handle.join().expect("Unable to join handle.");
    }
}

#[fixture]
#[once]
fn tempdir_fixture() -> TempDir {
    TempDir::new().unwrap()
}

#[fixture]
#[once]
fn make_entities(
    tempdir_fixture: &TempDir,
) -> (String, Arc<Mutex<NativeFileStore>>, JoD<()>, JoD<()>) {
    let entity_map = {
        let mut temp = HashMap::new();
        temp.insert(
            EntityID::from(0_u16),
            "127.0.0.1:55345"
                .to_socket_addrs()
                .expect("Improperly Formatted socket Address.")
                .next()
                .unwrap(),
        );
        temp.insert(
            EntityID::from(1_u16),
            "127.0.0.1:55346"
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
                UdpTransport::new("127.0.0.1:55346", entity_map.clone())
                    .expect("Unable to make UdpTransport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);

    let local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>> =
        HashMap::from([(
            vec![EntityID::from(1_u16)],
            Box::new(
                UdpTransport::new("127.0.0.1:55345", entity_map)
                    .expect("Unable to make UdpTransport."),
            ) as Box<dyn PDUTransport + Send>,
        )]);
    let temp_dir = tempdir_fixture;

    let utf8_path = Utf8PathBuf::from(
        temp_dir
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );

    let filestore = Arc::new(Mutex::new(NativeFileStore::new(&utf8_path)));
    filestore
        .lock()
        .unwrap()
        .create_directory("local")
        .expect("Unable to create local directory.");
    filestore
        .lock()
        .unwrap()
        .create_directory("remote")
        .expect("Unable to create local directory.");

    let data_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("data");
    for filename in ["small.txt", "medium.txt", "large.txt"] {
        fs::copy(
            data_dir.join(&filename),
            utf8_path.join("local").join(&filename),
        )
        .expect("Unable to copy file.");
    }

    let config = EntityConfig {
        fault_handler_override: HashMap::from([]),
        file_size_segment: 1024,
        default_transaction_max_count: 150,
        default_inactivity_timeout: 30,
        crc_flag: CRCFlag::NotPresent,
        closure_requested: false,
        checksum_type: ChecksumType::Modular,
    };

    let remote_config = {
        let mut temp = HashMap::new();

        temp.insert(EntityID::from(0_u16), config.clone());
        temp.insert(EntityID::from(1_u16), config.clone());
        temp
    };
    // Boolean to track if a kill signal is received
    let terminate = Arc::new(AtomicBool::new(false));

    for sig in TERM_SIGNALS {
        // When terminated by a second term signal, exit with exit code 1.
        // This will do nothing the first time (because term_now is false).
        flag::register_conditional_shutdown(*sig, 1, Arc::clone(&terminate))
            .expect("Unable to register termination signals.");
        // But this will "arm" the above for the second time, by setting it to true.
        // The order of registering these is important, if you put this one first, it will
        // first arm and then terminate ‒ all in the first round.
        flag::register(*sig, Arc::clone(&terminate))
            .expect("Unable to register termination signals.");
    }
    let local_signal = terminate.clone();
    let local_path = utf8_path.join("cfdp_local.socket").as_str().to_owned();
    let local_filestore = filestore.clone();

    let mut local_daemon = Daemon::new(
        EntityID::from(0_u16),
        TransactionSeqNum::from(0_u16),
        local_transport_map,
        local_filestore,
        remote_config.clone(),
        config.clone(),
        local_signal,
        Some(&local_path),
    )
    .expect("Cannot create daemon listener.");

    let local_handle = thread::Builder::new()
        .name("Local Daemon".to_string())
        .spawn(move || {
            local_daemon.manage_transactions().unwrap();
        })
        .expect("Unable to spwan local.");
    let remote_path = utf8_path.join("cfdp_remote.socket").as_str().to_owned();

    let remote_signal = terminate.clone();
    let remote_filestore = filestore.clone();
    let mut remote_daemon = Daemon::new(
        EntityID::from(1_u16),
        TransactionSeqNum::from(0_u16),
        remote_transport_map,
        remote_filestore,
        remote_config,
        config,
        remote_signal,
        Some(&remote_path),
    )
    .expect("Cannot create daemon listener.");

    let remote_handle = thread::Builder::new()
        .name("Remote Daemon".to_string())
        .spawn(move || {
            remote_daemon.manage_transactions().unwrap();
        })
        .expect("Unable to spawn remote.");

    let _local_h = JoD::from((local_handle, terminate.clone()));
    let _remote_h: JoD<_> = JoD::from((remote_handle, terminate));

    (local_path, filestore, _local_h, _remote_h)
}

#[fixture]
#[once]
fn get_filestore(
    make_entities: &'static (String, Arc<Mutex<NativeFileStore>>, JoD<()>, JoD<()>),
) -> (&'static String, Arc<Mutex<NativeFileStore>>) {
    (&make_entities.0, make_entities.1.clone())
}

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
