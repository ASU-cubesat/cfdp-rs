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

        fn send(&mut self, primitive: UserPrimitive) -> Result<(), IoError> {
            let mut connection = LocalSocketStream::connect(self.socket.as_str())?;
            connection.write_all(primitive.encode().as_slice())
        }

        /// Send the input [PutRequest] to the daemon to initiate a CFDP transfer
        pub fn put(&mut self, request: PutRequest) -> Result<TransactionID, IoError> {
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
        pub fn suspend(&mut self, transaction: TransactionID) -> Result<(), IoError> {
            self.send(UserPrimitive::Suspend(transaction.0, transaction.1))
        }

        /// Resume the input transaction
        pub fn resume(&mut self, transaction: TransactionID) -> Result<(), IoError> {
            self.send(UserPrimitive::Resume(transaction.0, transaction.1))
        }

        /// Cancel the input transaction
        pub fn cancel(&mut self, transaction: TransactionID) -> Result<(), IoError> {
            self.send(UserPrimitive::Cancel(transaction.0, transaction.1))
        }

        /// Receive the latest [Report] for the given transaction.
        pub fn report(&mut self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
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
                }
            }
            Ok(())
        }
    }
}
