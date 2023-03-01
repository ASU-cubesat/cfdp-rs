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

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::{Daemon, EntityConfig, PutRequest, Report, UserPrimitive},
    filestore::{ChecksumType, FileStore, NativeFileStore},
    pdu::{
        CRCFlag, Condition, EntityID, FaultHandlerAction, PDUDirective, PDUEncode, PDUPayload,
        TransactionSeqNum, VariableID, PDU,
    },
    transaction::TransactionID,
    transport::{PDUTransport, UdpTransport},
    user::{User, UserReturn},
};
use crossbeam_channel::{bounded, Receiver, Sender};
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

pub(crate) struct TestUser {
    internal_tx: Sender<(UserPrimitive, Sender<UserReturn>)>,
    internal_rx: Receiver<(UserPrimitive, Sender<UserReturn>)>,
}
impl TestUser {
    pub(crate) fn new() -> Self {
        let (internal_tx, internal_rx) = bounded(1);
        Self {
            internal_tx,
            internal_rx,
        }
    }

    pub(crate) fn split(self) -> (TestUserHalf, TestDaemonHalf) {
        let TestUser {
            internal_tx,
            internal_rx,
        } = self;
        (TestUserHalf { internal_tx }, TestDaemonHalf { internal_rx })
    }
}

#[derive(Debug, Clone)]
pub struct TestUserHalf {
    internal_tx: Sender<(UserPrimitive, Sender<UserReturn>)>,
}
impl TestUserHalf {
    pub fn put(&self, request: PutRequest) -> Result<TransactionID, IoError> {
        let primitive = UserPrimitive::Put(request);

        let (send, recv) = bounded(0);

        self.internal_tx.send((primitive, send)).map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })?;
        let response = recv.recv().map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })?;
        match response {
            UserReturn::ID(id) => Ok(id),
            _ => unreachable!(),
        }
    }

    // this function is actually used in series_f1 but series_f2 and f3 generate an unused warning
    // apparently related https://github.com/rust-lang/rust/issues/46379
    #[allow(unused)]
    pub fn cancel(&self, transaction: TransactionID) -> Result<(), IoError> {
        let primitive = UserPrimitive::Cancel(transaction.0, transaction.1);
        let (send, _recv) = bounded(0);
        self.internal_tx.send((primitive, send)).map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })
    }

    pub fn report(&self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
        let primitive = UserPrimitive::Report(transaction.0, transaction.1);
        let (send, recv) = bounded(0);

        self.internal_tx.send((primitive, send)).map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })?;
        let response = recv.recv().map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })?;
        match response {
            UserReturn::Report(report) => Ok(report),
            _ => unreachable!(),
        }
    }
}

pub(crate) struct TestDaemonHalf {
    internal_rx: Receiver<(UserPrimitive, Sender<UserReturn>)>,
}
impl User for TestDaemonHalf {
    fn primitive_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<(UserPrimitive, Sender<UserReturn>)>,
    ) -> Result<(), IoError> {
        while !signal.load(Ordering::Relaxed) {
            match self.internal_rx.recv_timeout(Duration::from_millis(250)) {
                Ok((primitive, internal_return)) => {
                    let (internal_send, internal_recv) = bounded(0);

                    match primitive {
                        UserPrimitive::Put(_) => {
                            sender.send((primitive, internal_send)).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?;
                            let response = internal_recv.recv().map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?;

                            internal_return.send(response).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?;
                        }
                        UserPrimitive::Cancel(_, _) => {
                            sender.send((primitive, internal_send)).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?
                        }
                        UserPrimitive::Suspend(_, _) => {
                            sender.send((primitive, internal_send)).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?
                        }
                        UserPrimitive::Resume(_, _) => {
                            sender.send((primitive, internal_send)).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?
                        }
                        UserPrimitive::Report(_, _) => {
                            sender.send((primitive, internal_send)).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?;
                            let response = internal_recv.recv().map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?;

                            internal_return.send(response).map_err(|_| {
                                IoError::new(
                                    ErrorKind::ConnectionReset,
                                    "Daemon Half of User disconnected.",
                                )
                            })?;
                        }
                    };
                }
                Err(_err) => {}
            }
        }
        Ok(())
    }
}

impl<'a, T> Drop for JoD<'a, T> {
    fn drop(&mut self) {
        self.signal.store(true, Ordering::Relaxed);
        let handle = self.handle.remove(0);

        handle.join().expect("Unable to join handle.");
    }
}

// Returns the local user, remote user, and handles for local and remote daemons.
type DaemonType = (
    TestUserHalf,
    TestUserHalf,
    JoD<'static, Result<(), String>>,
    JoD<'static, Result<(), String>>,
);

// Inactivity, ACK, NAK
type Timeouts = [Option<i64>; 3];

#[allow(clippy::too_many_arguments)]
pub(crate) fn create_daemons<T: FileStore + Sync + Send + 'static>(
    filestore: Arc<T>,
    local_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    remote_transport_map: HashMap<Vec<EntityID>, Box<dyn PDUTransport + Send>>,
    signal: Arc<AtomicBool>,
    timeouts: Timeouts,
) -> DaemonType {
    let config = EntityConfig {
        fault_handler_override: HashMap::from([(
            Condition::PositiveLimitReached,
            FaultHandlerAction::Abandon,
        )]),
        file_size_segment: 1024,
        default_transaction_max_count: 2,
        inactivity_timeout: timeouts[0].unwrap_or(1),
        ack_timeout: timeouts[1].unwrap_or(1),
        nak_timeout: timeouts[2].unwrap_or(1),
        crc_flag: CRCFlag::NotPresent,
        closure_requested: false,
        checksum_type: ChecksumType::Modular,
    };

    let remote_config = HashMap::from([
        (EntityID::from(0_u16), config.clone()),
        (EntityID::from(1_u16), config.clone()),
    ]);

    let local_filestore = filestore.clone();

    let local_user = TestUser::new();
    let (local_userhalf, local_daemonhalf) = local_user.split();

    let mut local_daemon = Daemon::new(
        EntityID::from(0_u16),
        TransactionSeqNum::from(0_u16),
        local_transport_map,
        local_filestore,
        remote_config.clone(),
        config.clone(),
        signal.clone(),
        Box::new(local_daemonhalf),
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

    let remote_user = TestUser::new();
    let (remote_userhalf, remote_daemonhalf) = remote_user.split();

    let remote_filestore = filestore;
    let mut remote_daemon = Daemon::new(
        EntityID::from(1_u16),
        TransactionSeqNum::from(0_u16),
        remote_transport_map,
        remote_filestore,
        remote_config,
        config,
        signal.clone(),
        Box::new(remote_daemonhalf),
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

    (local_userhalf, remote_userhalf, _local_h, _remote_h)
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
// Returns the local user, remote user, filestore, and handles for both local and remote daemons.
pub(crate) type EntityConstructorReturn = (
    TestUserHalf,
    TestUserHalf,
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
            data_dir.join(filename),
            utf8_path.join("local").join(filename),
        )
        .expect("Unable to copy file.");
    }
    let (local_user, remote_user, local_handle, remote_handle) = create_daemons(
        filestore.clone(),
        local_transport_map,
        remote_transport_map,
        terminate.clone(),
        [None, None, None],
    );
    (
        local_user,
        remote_user,
        filestore,
        local_handle,
        remote_handle,
    )
}

pub(crate) type UsersAndFilestore = (TestUserHalf, TestUserHalf, Arc<NativeFileStore>);
#[fixture]
#[once]
pub(crate) fn get_filestore(make_entities: &'static EntityConstructorReturn) -> UsersAndFilestore {
    (
        make_entities.0.clone(),
        make_entities.1.clone(),
        make_entities.2.clone(),
    )
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
    // Recreates inactivity at sender
    Inactivity,
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
                            if self.counter == 1 && operation.get_directive() == *skip_directive {
                                self.counter += 1;
                                Ok(())
                            } else {
                                if operation.get_directive() == *skip_directive {}
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
                                (1, PDUDirective::EoF) => {
                                    self.counter += 1;
                                    self.socket
                                        .send_to(pdu.encode().as_slice(), addr)
                                        .map(|_n| ())
                                }
                                (1, PDUDirective::Ack) => {
                                    self.counter += 1;
                                    // increment counter but still don't send it
                                    Ok(())
                                }
                                (1, _) => Ok(()),
                                (_, _) => self
                                    .socket
                                    .send_to(pdu.encode().as_slice(), addr)
                                    .map(|_n| ()),
                            }
                        }
                        PDUPayload::FileData(_data) => {
                            if self.counter == 1 {
                                Ok(())
                            } else {
                                self.socket
                                    .send_to(pdu.encode().as_slice(), addr)
                                    .map(|_n| ())
                            }
                        }
                    },
                    TransportIssue::Inactivity => {
                        // Send the Metadata PDU only, and nothing else.
                        if self.counter == 1 {
                            self.counter += 1;
                            self.socket
                                .send_to(pdu.encode().as_slice(), addr)
                                .map(|_n| ())
                        } else {
                            Ok(())
                        }
                    }
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
                                error!("Transport found disconnect sending channel: {error}");
                                return Err(IoError::from(ErrorKind::ConnectionAborted));
                            }
                        };
                    }
                    Err(error) => {
                        error!("Error decoding PDU: {error}");
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
                    return Err(e);
                }
            };
            match recv.try_recv() {
                Ok((entity, pdu)) => self.request(entity, pdu)?,
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // nothing to do here
                }
                Err(err @ crossbeam_channel::TryRecvError::Disconnected) => {
                    error!("Transport found disconnected channel: {err}");
                    return Err(IoError::from(ErrorKind::ConnectionAborted));
                }
            };
            thread::sleep(Duration::from_micros(500))
        }
        Ok(())
    }
}
