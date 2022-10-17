use std::{
    collections::HashMap,
    fs,
    io::{Error as IoError, ErrorKind},
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use camino::{Utf8Path, Utf8PathBuf};
use cfdp_core::{
    daemon::{Daemon, EntityConfig},
    filestore::{ChecksumType, FileStore, NativeFileStore},
    pdu::{
        CRCFlag, Condition, EntityID, FaultHandlerAction, PDUDirective, PDUEncode, PDUPayload,
        TransactionSeqNum, VariableID, PDU,
    },
    transport::{PDUTransport, UdpTransport},
};
use crossbeam_channel::{Receiver, Sender};
use log::error;
use signal_hook::{consts::TERM_SIGNALS, flag};
use tempfile::TempDir;

use rstest::fixture;
#[derive(Debug)]
pub(crate) struct JoD<'a, T> {
    handle: Vec<JoinHandle<T>>,
    signal: Arc<AtomicBool>,
    phantom: PhantomData<&'a ()>,
}
impl<'a, T> From<(JoinHandle<T>, Arc<AtomicBool>)> for JoD<'a, T> {
    fn from(input: (JoinHandle<T>, Arc<AtomicBool>)) -> Self {
        Self {
            handle: vec![input.0],
            signal: input.1,
            phantom: PhantomData,
        }
    }
}

impl<'a, T> Drop for JoD<'a, T> {
    fn drop(&mut self) {
        self.signal.store(true, Ordering::Relaxed);
        let handle = self.handle.remove(0);

        handle.join().expect("Unable to join handle.");
    }
}

type DaemonType = (
    String,
    JoD<'static, Result<(), String>>,
    JoD<'static, Result<(), String>>,
);

pub(crate) fn create_daemons<T: FileStore + Sync + Send + 'static>(
    utf8_path: &Utf8Path,
    filestore: Arc<T>,
    local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    local_socket: &str,
    remote_socket: &str,
    signal: Arc<AtomicBool>,
) -> DaemonType {
    let config = EntityConfig {
        fault_handler_override: HashMap::from([(
            Condition::PositiveLimitReached,
            FaultHandlerAction::Abandon,
        )]),
        file_size_segment: 1024,
        default_transaction_max_count: 2,
        default_inactivity_timeout: 1,
        crc_flag: CRCFlag::NotPresent,
        closure_requested: false,
        checksum_type: ChecksumType::Modular,
    };

    let remote_config = HashMap::from([
        (EntityID::from(0_u16), config.clone()),
        (EntityID::from(1_u16), config.clone()),
    ]);

    let local_path = utf8_path.join(local_socket).as_str().to_owned();
    let local_filestore = filestore.clone();

    let mut local_daemon = Daemon::new(
        EntityID::from(0_u16),
        TransactionSeqNum::from(0_u16),
        local_transport_map,
        local_filestore,
        remote_config.clone(),
        config.clone(),
        signal.clone(),
        Some(&local_path),
    )
    .expect("Cannot create daemon listener.");

    let local_handle = thread::Builder::new()
        .name("Local Daemon".to_string())
        .spawn(move || {
            local_daemon
                .manage_transactions()
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .expect("Unable to spwan local.");
    let remote_path = utf8_path.join(remote_socket).as_str().to_owned();

    let remote_filestore = filestore;
    let mut remote_daemon = Daemon::new(
        EntityID::from(1_u16),
        TransactionSeqNum::from(0_u16),
        remote_transport_map,
        remote_filestore,
        remote_config,
        config,
        signal.clone(),
        Some(&remote_path),
    )
    .expect("Cannot create daemon listener.");

    let remote_handle = thread::Builder::new()
        .name("Remote Daemon".to_string())
        .spawn(move || {
            remote_daemon
                .manage_transactions()
                .map_err(|e| e.to_string())?;
            Ok(())
        })
        .expect("Unable to spawn remote.");

    let _local_h = JoD::from((local_handle, signal.clone()));
    let _remote_h: JoD<_> = JoD::from((remote_handle, signal));

    (local_path, _local_h, _remote_h)
}

#[fixture]
#[once]
pub(crate) fn tempdir_fixture() -> TempDir {
    TempDir::new().unwrap()
}

#[fixture]
#[once]
pub(crate) fn terminate() -> Arc<AtomicBool> {
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
    terminate
}

pub(crate) type EntityConstructorReturn = (
    String,
    Arc<NativeFileStore>,
    JoD<'static, Result<(), String>>,
    JoD<'static, Result<(), String>>,
);

#[fixture]
#[once]
fn make_entities(
    tempdir_fixture: &TempDir,
    terminate: &Arc<AtomicBool>,
) -> EntityConstructorReturn {
    let remote_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind remote UDP.");
    let remote_addr = remote_udp.local_addr().expect("Cannot find local address.");

    let local_udp = UdpSocket::bind("127.0.0.1:0").expect("Unable to bind local UDP.");
    let local_addr = local_udp.local_addr().expect("Cannot find local address.");

    let entity_map = HashMap::from([
        (EntityID::from(0_u16), local_addr),
        (EntityID::from(1_u16), remote_addr),
    ]);

    let local_transport = UdpTransport::try_from((local_udp, entity_map.clone()))
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

    let utf8_path = Utf8PathBuf::from(
        tempdir_fixture
            .path()
            .as_os_str()
            .to_str()
            .expect("Unable to coerce tmp path to String."),
    );

    let filestore = Arc::new(NativeFileStore::new(&utf8_path));
    filestore
        .create_directory("local")
        .expect("Unable to create local directory.");
    filestore
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
    let (path, local, remote) = create_daemons(
        utf8_path.as_path(),
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        "cfdp_local.socket",
        "cfdp_remote.socket",
        terminate.clone(),
    );
    (path, filestore, local, remote)
}

#[fixture]
#[once]
pub(crate) fn get_filestore(
    make_entities: &'static EntityConstructorReturn,
) -> (&'static String, Arc<NativeFileStore>) {
    (&make_entities.0, make_entities.1.clone())
}

#[allow(dead_code)]
pub(crate) enum TransportIssue {
    // Every Nth packet will be dropped
    Rate(usize),
    // Every Nth packet will be duplicated
    Duplicate(usize),
    // Stores eveyt Nth  PDU for sending out of order
    Reorder(usize),
    // This specific PDU is dropped the first time it is sent.
    Once(PDUDirective),
    // This PDU type is dropped every time,
    All(Vec<PDUDirective>),
    // Every singel PDU should be dropped once.
    // except for EoF
    Every,
}
pub(crate) struct LossyTransport {
    pub(crate) socket: UdpSocket,
    entity_map: HashMap<VariableID, SocketAddr>,
    counter: usize,
    issue: TransportIssue,
    buffer: Vec<PDU>,
}
impl LossyTransport {
    #[allow(dead_code)]
    pub fn new<T: ToSocketAddrs>(
        addr: T,
        entity_map: HashMap<VariableID, SocketAddr>,
        issue: TransportIssue,
    ) -> Result<Self, IoError> {
        let socket = UdpSocket::bind(addr)?;
        socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket,
            entity_map,
            counter: 1,
            issue,
            buffer: vec![],
        })
    }
}
impl TryFrom<(UdpSocket, HashMap<VariableID, SocketAddr>, TransportIssue)> for LossyTransport {
    type Error = IoError;

    fn try_from(
        inputs: (UdpSocket, HashMap<VariableID, SocketAddr>, TransportIssue),
    ) -> Result<Self, Self::Error> {
        let me = Self {
            socket: inputs.0,
            entity_map: inputs.1,
            counter: 1,
            issue: inputs.2,
            buffer: vec![],
        };
        me.socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        me.socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        me.socket.set_nonblocking(true)?;
        Ok(me)
    }
}
impl PDUTransport for LossyTransport {
    fn is_ready(&self) -> bool {
        self.socket.local_addr().is_ok()
    }

    fn request(&mut self, destination: VariableID, pdu: PDU) -> Result<(), IoError> {
        self.entity_map
            .get(&destination)
            .ok_or_else(|| IoError::from(ErrorKind::AddrNotAvailable))
            .and_then(|addr| {
                // send a delayed packet if there are any
                if !self.buffer.is_empty() {
                    let pdu = self.buffer.remove(0);
                    self.socket
                        .send_to(pdu.encode().as_slice(), addr)
                        .map(|_n| ())?;
                }
                match &self.issue {
                    TransportIssue::Rate(rate) => {
                        if self.counter % rate == 0 {
                            self.counter += 1;
                            Ok(())
                        } else {
                            self.counter += 1;
                            self.socket
                                .send_to(pdu.encode().as_slice(), addr)
                                .map(|_n| ())
                        }
                    }
                    TransportIssue::Duplicate(rate) => {
                        if self.counter % rate == 0 {
                            self.counter += 1;
                            self.socket
                                .send_to(pdu.clone().encode().as_slice(), addr)
                                .map(|_n| ())?;
                            self.socket
                                .send_to(pdu.encode().as_slice(), addr)
                                .map(|_n| ())
                        } else {
                            self.counter += 1;
                            self.socket
                                .send_to(pdu.encode().as_slice(), addr)
                                .map(|_n| ())
                        }
                    }
                    TransportIssue::Reorder(rate) => {
                        if self.counter % rate == 0 {
                            self.counter += 1;
                            self.buffer.push(pdu);
                            Ok(())
                        } else {
                            self.counter += 1;
                            self.socket
                                .send_to(pdu.encode().as_slice(), addr)
                                .map(|_n| ())
                        }
                    }
                    TransportIssue::Once(skip_directive) => match &pdu.payload {
                        PDUPayload::Directive(operation) => {
                            if self.counter == 0 && &operation.get_directive() == skip_directive {
                                self.counter += 1;
                                Ok(())
                            } else {
                                self.socket
                                    .send_to(pdu.encode().as_slice(), addr)
                                    .map(|_n| ())
                            }
                        }
                        PDUPayload::FileData(_data) => self
                            .socket
                            .send_to(pdu.encode().as_slice(), addr)
                            .map(|_n| ()),
                    },
                    TransportIssue::All(skip_directive) => match &pdu.payload {
                        PDUPayload::Directive(operation) => {
                            if skip_directive.contains(&operation.get_directive()) {
                                Ok(())
                            } else {
                                self.socket
                                    .send_to(pdu.encode().as_slice(), addr)
                                    .map(|_n| ())
                            }
                        }
                        PDUPayload::FileData(_data) => self
                            .socket
                            .send_to(pdu.encode().as_slice(), addr)
                            .map(|_n| ()),
                    },
                    // only drop the PDUs if we have not yet send EoF.
                    // Flip the counter on EoF to signify we can send again.
                    TransportIssue::Every => match &pdu.payload {
                        PDUPayload::Directive(operation) => {
                            match (self.counter, operation.get_directive()) {
                                (0, PDUDirective::EoF) => {
                                    self.counter += 1;
                                    self.socket
                                        .send_to(pdu.encode().as_slice(), addr)
                                        .map(|_n| ())
                                }
                                (0, _) => Ok(()),
                                (_, _) => self
                                    .socket
                                    .send_to(pdu.encode().as_slice(), addr)
                                    .map(|_n| ()),
                            }
                        }
                        PDUPayload::FileData(_data) => {
                            if self.counter == 0 {
                                Ok(())
                            } else {
                                self.socket
                                    .send_to(pdu.encode().as_slice(), addr)
                                    .map(|_n| ())
                            }
                        }
                    },
                }
            })
    }

    fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        recv: Receiver<(VariableID, PDU)>,
    ) -> Result<(), IoError> {
        // this buffer will be 511 KiB, should be sufficiently small;
        let mut buffer = vec![0_u8; u16::MAX as usize];
        while !signal.load(Ordering::Relaxed) {
            match self.socket.recv_from(&mut buffer) {
                Ok(_n) => match PDU::decode(&mut buffer.as_slice()) {
                    Ok(pdu) => {
                        match sender.send(pdu) {
                            Ok(()) => {}
                            Err(error) => {
                                println!("Transport found disconnect sending channel: {}", error);
                                error!("Transport found disconnect sending channel: {}", error);
                                return Err(IoError::from(ErrorKind::ConnectionAborted));
                            }
                        };
                    }
                    Err(error) => {
                        error!("Error decoding PDU: {}", error);
                        println!("Error decoding PDU: {}", error);
                        // might need to stop depending on the error.
                        // some are recoverable though
                    }
                },
                Err(ref e)
                    if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut =>
                {
                    // continue to trying to send
                }
                Err(e) => {
                    error!("encountered IO error: {e}");
                    println!("encountered IO error: {e}");
                    return Err(e);
                }
            };
            match recv.try_recv() {
                Ok((entity, pdu)) => self.request(entity, pdu)?,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // nothing to do here
                }
                Err(err @ crossbeam_channel::TryRecvError::Disconnected) => {
                    error!("Transport found disconnected channel: {}", err);
                    println!("Transport found disconnected channel: {}", err);
                    return Err(IoError::from(ErrorKind::ConnectionAborted));
                }
            };
            thread::sleep(Duration::from_micros(500))
        }
        Ok(())
    }
}
