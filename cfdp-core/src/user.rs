use std::{
    io::Error as IoError,
    sync::{atomic::AtomicBool, Arc},
};

use crate::{
    daemon::{Report, UserPrimitive},
    transaction::TransactionID,
};

use crossbeam_channel::Sender;

#[derive(Debug)]
/// Some User interactions require the ID to be returned to the user
/// or the status report requested.
// this enum allows us to send a one-off channel to the Daemon
// then listen for the response.
pub enum UserReturn {
    ID(TransactionID),
    Report(Option<Report>),
}

/// The CFDP user is an interface to initiate transactions
/// and receive status updates for users. It interfaces with the [Daemon](crate::daemon::Daemon)
/// through a channel to relay any input [UserPrimitive](crate::daemon::UserPrimitive) and recieve any requested [Report](crate::daemon::Report).
pub trait User {
    /// Provides any logic for communicating UserPrimitives to the [Daemon](crate::daemon::Daemon).

    /// A User implementation will listen for any incoming [UserPrimitive](crate::daemon::UserPrimitive)
    /// from its user-facing counterpart (implementation specific)
    /// forward it over the sender channel to the [Daemon](crate::daemon::Daemon).
    /// Any requested [Report](crate::daemon::Report) is returned on the receiving channel.
    /// The signal is used to indicate a shutdown operation was requested.
    fn primitive_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<(UserPrimitive, Sender<UserReturn>)>,
        // recv: Receiver<Report>,
    ) -> Result<(), IoError>;
}

#[cfg(feature = "ipc")]
pub use ipc::IpcUser;

#[cfg(feature = "ipc")]
mod ipc {

    #[cfg(windows)]
    pub(crate) const SOCKET_ADDR: &str = "cfdp";
    #[cfg(not(windows))]
    pub const SOCKET_ADDR: &str = "/var/run/cfdp.socket";

    use camino::Utf8PathBuf;
    use crossbeam_channel::{bounded, Sender};
    use interprocess::local_socket::{LocalSocketListener, LocalSocketStream};
    use log::error;
    use std::{
        fs,
        io::{Error as IoError, ErrorKind, Read, Write},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::Duration,
    };

    use crate::{
        daemon::{PutRequest, Report, UserPrimitive},
        pdu::{EntityID, PDUEncode, TransactionSeqNum},
        transaction::TransactionID,
    };

    use super::{User, UserReturn};

    /// A default implementation of the [User] trait which uses [LocalSocketListener]
    /// to communicate between a user facing command line process and the internal User implemenation.
    #[derive(Clone, Debug)]
    pub struct IpcUser {
        socket: Utf8PathBuf,
    }
    impl IpcUser {
        /// Create a new IpcUser instance at the provided socket address.
        /// The address must be compatible with [Utf8PathBuf] .
        pub fn new(socket: Option<&(impl AsRef<str> + ?Sized)>) -> Result<Self, IoError> {
            Ok(Self {
                socket: socket
                    .map(Utf8PathBuf::from)
                    .unwrap_or_else(|| Utf8PathBuf::from(SOCKET_ADDR)),
            })
        }

        fn send(&self, primitive: UserPrimitive) -> Result<(), IoError> {
            let mut connection = LocalSocketStream::connect(self.socket.as_str())?;
            connection.write_all(primitive.encode().as_slice())
        }

        /// Send the input [PutRequest] to the daemon to initiate a CFDP transfer
        pub fn put(&self, request: PutRequest) -> Result<TransactionID, IoError> {
            let primitive = UserPrimitive::Put(request);
            let id = {
                let mut connection = LocalSocketStream::connect(self.socket.as_str())?;
                connection.write_all(primitive.encode().as_slice())?;

                (
                    EntityID::decode(&mut connection)
                        .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
                    TransactionSeqNum::decode(&mut connection)
                        .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
                )
            };
            Ok(id)
        }

        /// Suspend the input transactin
        pub fn suspend(&self, transaction: TransactionID) -> Result<(), IoError> {
            self.send(UserPrimitive::Suspend(transaction.0, transaction.1))
        }

        /// Resume the input transaction
        pub fn resume(&self, transaction: TransactionID) -> Result<(), IoError> {
            self.send(UserPrimitive::Resume(transaction.0, transaction.1))
        }

        /// Cancel the input transaction
        pub fn cancel(&self, transaction: TransactionID) -> Result<(), IoError> {
            self.send(UserPrimitive::Cancel(transaction.0, transaction.1))
        }

        /// Receive the latest [Report] for the given transaction.
        pub fn report(&self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
            let primitive = UserPrimitive::Report(transaction.0, transaction.1);

            let report = {
                let mut connection = LocalSocketStream::connect(self.socket.as_str())?;

                connection.write_all(primitive.encode().as_slice())?;

                {
                    let mut u64_buff = [0_u8; 8];
                    connection.read_exact(&mut u64_buff)?;

                    match u64::from_be_bytes(u64_buff) {
                        0 => None,
                        _ => Some(
                            Report::decode(&mut connection)
                                .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
                        ),
                    }
                }
            };
            match &report {
                Some(data) => println!(
                    "Status of Transaction ({:?}, {:?}). State: {:?}. Status: {:?}. Condition: {:?}.",
                    data.id.0, data.id.1, data.state, data.status, data.condition
                ),
                None => println!(
                    "No Report available for ({:?} , {:?})",
                    transaction.0, transaction.1
                ),
            }

            Ok(report)
        }
    }
    impl User for IpcUser {
        fn primitive_handler(
            &mut self,
            signal: Arc<AtomicBool>,
            sender: Sender<(UserPrimitive, Sender<UserReturn>)>,
        ) -> Result<(), IoError> {
            if self.socket.exists() {
                fs::remove_file(self.socket.as_path())?;
            }

            let listener = LocalSocketListener::bind(self.socket.as_std_path())?;
            // setting to non-blocking lets us grab conections that are open
            // without blocking the entire thread.
            listener.set_nonblocking(true)?;
            while !signal.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok(mut conn) => {
                        let (internal_send, internal_recv) = bounded(0);
                        let primitive = UserPrimitive::decode(&mut conn).map_err(|_| {
                            IoError::new(
                                ErrorKind::InvalidData,
                                "Unable to parse user primitive from connected socket.",
                            )
                        })?;
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
                                if let UserReturn::ID(id) = response {
                                    conn.write_all(
                                        [id.0.encode(), id.1.encode()].concat().as_slice(),
                                    )?;
                                }
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
                                let vec = match response {
                                    UserReturn::Report(report) => match report {
                                        Some(inner) => inner.encode(),
                                        None => vec![],
                                    },
                                    _ => unreachable!(),
                                };
                                conn.write_all(
                                    [(vec.len() as u64).to_be_bytes().to_vec(), vec]
                                        .concat()
                                        .as_slice(),
                                )?;
                            }
                        };
                    }
                    Err(_err)
                        if _err.kind() == ErrorKind::WouldBlock
                            || _err.kind() == ErrorKind::TimedOut =>
                    {
                        thread::sleep(Duration::from_millis(100))
                    }
                    Err(e) => {
                        error!("encountered IO error: {e}");
                        return Err(e);
                    }
                };
            }
            Ok(())
        }
    }

    // interprocess has some problems on MacOS and Windows
    #[cfg(all(test, target_os = "linux"))]
    mod test {
        use std::thread::JoinHandle;

        use crate::{
            pdu::{Condition, TransactionStatus, TransmissionMode, VariableID},
            transaction::TransactionState,
        };

        use super::*;

        use crossbeam_channel::Receiver;
        use rstest::{fixture, rstest};
        use signal_hook::{consts::TERM_SIGNALS, flag};
        use tempfile::TempDir;

        type IpcDaemonType = (
            IpcUser,
            JoinHandle<()>,
            Receiver<(UserPrimitive, Sender<UserReturn>)>,
        );

        #[fixture]
        #[once]
        fn tempdir() -> TempDir {
            TempDir::new().unwrap()
        }

        #[fixture]
        fn daemon_ipc(#[default("ipc.socket")] name: &str, tempdir: &TempDir) -> IpcDaemonType {
            let directory = tempdir;
            let socket_path = directory.path().join(name);
            let str_name = socket_path.as_os_str().to_str().unwrap();
            let ipc_user = IpcUser::new(Some(str_name)).expect("unable to create daemon half");
            let mut daemon_user = ipc_user.clone();

            let (sender, receiver) = bounded(1);

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
            let signal = terminate;
            let handle = thread::spawn(move || {
                daemon_user
                    .primitive_handler(signal, sender)
                    .expect("Err in daemon user half.");
            });
            thread::sleep(Duration::from_millis(5));
            assert!(ipc_user.socket.as_std_path().exists());

            (ipc_user, handle, receiver)
        }

        #[rstest]
        fn put(#[with("put")] daemon_ipc: IpcDaemonType) {
            let (user, _handle, receiver) = daemon_ipc;
            let expected = PutRequest {
                source_filename: "input_name".into(),
                destination_filename: "output_name".into(),
                destination_entity_id: VariableID::from(12_u16),
                transmission_mode: TransmissionMode::Acknowledged,
                filestore_requests: vec![],
                message_to_user: vec![],
            };
            println!("{:?}", user.socket);
            let req_out = expected.clone();
            thread::spawn(move || {
                user.put(req_out).expect("unable to send put request");
            });

            let received = receiver.recv().expect("unable to get primitive.");
            if let UserPrimitive::Put(req_received) = received.0 {
                assert_eq!(expected, req_received)
            } else {
                panic!()
            }
        }

        #[rstest]
        fn suspend(#[with("suspend")] daemon_ipc: IpcDaemonType) {
            let (user, _handle, receiver) = daemon_ipc;
            let expected: TransactionID = (VariableID::from(12_u16), VariableID::from(8_u16));
            thread::spawn(move || {
                user.suspend(expected)
                    .expect("unable to send suspend request");
            });

            let received = receiver.recv().expect("unable to get primitive.");
            if let UserPrimitive::Suspend(id, seq_num) = received.0 {
                assert_eq!(expected, (id, seq_num))
            } else {
                panic!()
            }
        }

        #[rstest]
        fn resume(#[with("resume")] daemon_ipc: IpcDaemonType) {
            let (user, _handle, receiver) = daemon_ipc;
            let expected: TransactionID = (VariableID::from(12_u16), VariableID::from(8_u16));

            thread::spawn(move || {
                user.resume(expected)
                    .expect("unable to send resume request");
            });

            let received = receiver.recv().expect("unable to get primitive.");
            if let UserPrimitive::Resume(id, seq_num) = received.0 {
                assert_eq!(expected, (id, seq_num))
            } else {
                panic!()
            }
        }

        #[rstest]
        fn cancel(#[with("cancel")] daemon_ipc: IpcDaemonType) {
            let (user, _handle, receiver) = daemon_ipc;
            let expected: TransactionID = (VariableID::from(12_u16), VariableID::from(8_u16));

            thread::spawn(move || {
                user.cancel(expected)
                    .expect("unable to send cancel request");
            });

            let received = receiver.recv().expect("unable to get primitive.");
            if let UserPrimitive::Cancel(id, seq_num) = received.0 {
                assert_eq!(expected, (id, seq_num))
            } else {
                panic!()
            }
        }

        #[rstest]
        fn report_some(#[with("report_some")] daemon_ipc: IpcDaemonType) {
            let (user, _handle, receiver) = daemon_ipc;
            let expected_id: TransactionID = (VariableID::from(12_u16), VariableID::from(8_u16));

            let expected = Report {
                id: expected_id,
                state: TransactionState::Active,
                status: TransactionStatus::Active,
                condition: Condition::NoError,
            };

            let inner_report = expected.clone();
            let handle = thread::spawn(move || {
                let report = user
                    .report(expected_id)
                    .expect("unable to send cancel request")
                    .unwrap();

                assert_eq!(inner_report, report)
            });

            let (received, report_sender) = receiver.recv().expect("unable to get primitive.");
            if let UserPrimitive::Report(id, seq_num) = received {
                assert_eq!(expected_id, (id, seq_num));
                report_sender
                    .send(UserReturn::Report(Some(expected)))
                    .expect("Cannot send response report");
                handle.join().expect("Error during user reporting");
            } else {
                panic!()
            }
        }

        #[rstest]
        fn report_none(#[with("report_none")] daemon_ipc: IpcDaemonType) {
            let (user, _handle, receiver) = daemon_ipc;
            let expected_id: TransactionID = (VariableID::from(12_u16), VariableID::from(8_u16));

            let expected = None;

            let inner_report = expected.clone();
            let handle = thread::spawn(move || {
                let report = user
                    .report(expected_id)
                    .expect("unable to send cancel request");

                assert_eq!(inner_report, report)
            });

            let (received, report_sender) = receiver.recv().expect("unable to get primitive.");
            if let UserPrimitive::Report(id, seq_num) = received {
                assert_eq!(expected_id, (id, seq_num));
                report_sender
                    .send(UserReturn::Report(expected))
                    .expect("Cannot send response report");
                handle.join().expect("Error during user reporting");
            } else {
                panic!()
            }
        }
    }
}
