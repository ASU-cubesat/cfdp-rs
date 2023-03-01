use std::{
    collections::HashMap,
    io::{Error as IoError, ErrorKind},
    net::{SocketAddr, UdpSocket as StdUdpSocket},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use async_trait::async_trait;
use log::{error, info};
use thiserror::Error;
#[cfg(feature = "uart")]
use tokio::io::AsyncWriteExt;
use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
};
#[cfg(feature = "uart")]
use tokio_serial::{Error as SerialError, SerialPort, SerialStream};

use crate::pdu::{PDUEncode, VariableID, PDU};

#[derive(Error, Debug)]
pub enum TransportError {
    #[error("IO error during transport operation: {0}")]
    Io(#[from] IoError),
    #[cfg(feature = "uart")]
    #[cfg_attr(feature = "uart", error("Serial port communcation error {0:}"))]
    Serial(#[from] SerialError),
}

/// Transports are designed to run in a thread in the background
/// inside a [Daemon](crate::daemon::Daemon) process
#[async_trait]
pub trait PDUTransport {
    /// Verify underyling communication method is ready.
    fn is_ready(&self) -> bool;

    /// Send input PDU to the remote
    /// The implementation must have a method to lookup an Entity's address from the ID
    async fn request(&mut self, destination: VariableID, pdu: PDU) -> Result<(), IoError>;

    /// Provides logic for listening for incoming PDUs and sending any outbound PDUs

    /// A transport implementation will send any received messages through the
    /// [Sender] channel to the [Daemon](crate::daemon::Daemon).
    /// The [Receiver] channel is used to recv PDUs from the Daemon and send them to their respective remote Entity.
    /// The [Daemon](crate::daemon::Daemon) is responsible for receiving messages and ditribute them to each
    /// [Transaction](crate::transaction::Transaction) as necessary.
    /// The signal is used to indicate a shutdown operation was requested.
    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: UnboundedSender<PDU>,
        recv: UnboundedReceiver<(VariableID, PDU)>,
    ) -> Result<(), IoError>;
}

/// A wrapper struct around a [UdpSocket] and a Mapping from
/// EntityIDs to [SocketAddr] instances.
pub struct UdpTransport {
    socket: UdpSocket,
    entity_map: HashMap<VariableID, SocketAddr>,
}
impl UdpTransport {
    pub async fn new<T: ToSocketAddrs>(
        addr: T,
        entity_map: HashMap<VariableID, SocketAddr>,
    ) -> Result<Self, IoError> {
        let socket = UdpSocket::bind(addr).await?;
        // socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        // socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        // socket.set_nonblocking(true)?;
        Ok(Self { socket, entity_map })
    }
}
impl TryFrom<(StdUdpSocket, HashMap<VariableID, SocketAddr>)> for UdpTransport {
    type Error = IoError;
    fn try_from(
        inputs: (StdUdpSocket, HashMap<VariableID, SocketAddr>),
    ) -> Result<Self, Self::Error> {
        inputs.0.set_nonblocking(true)?;
        let me = Self {
            socket: UdpSocket::from_std(inputs.0)?,
            entity_map: inputs.1,
        };
        // me.socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        // me.socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        Ok(me)
    }
}
impl TryFrom<(UdpSocket, HashMap<VariableID, SocketAddr>)> for UdpTransport {
    type Error = IoError;
    fn try_from(inputs: (UdpSocket, HashMap<VariableID, SocketAddr>)) -> Result<Self, Self::Error> {
        let me = Self {
            socket: inputs.0,
            entity_map: inputs.1,
        };
        // me.socket.set_read_timeout(Some(Duration::from_secs(1)))?;
        // me.socket.set_write_timeout(Some(Duration::from_secs(1)))?;
        // me.socket.set_nonblocking(true)?;
        Ok(me)
    }
}
#[async_trait()]
impl PDUTransport for UdpTransport {
    fn is_ready(&self) -> bool {
        self.socket.local_addr().is_ok()
    }

    async fn request(&mut self, destination: VariableID, pdu: PDU) -> Result<(), IoError> {
        let addr = self
            .entity_map
            .get(&destination)
            .ok_or_else(|| IoError::from(ErrorKind::AddrNotAvailable))?;

        self.socket
            .send_to(pdu.encode().as_slice(), addr)
            .await
            .map(|_n| ())
    }

    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: UnboundedSender<PDU>,
        mut recv: UnboundedReceiver<(VariableID, PDU)>,
    ) -> Result<(), IoError> {
        // this buffer will be 511 KiB, should be sufficiently small;
        let mut buffer = vec![0_u8; u16::MAX as usize];
        while !signal.load(Ordering::Relaxed) {
            tokio::select! {
                Ok((_n, _addr)) = self.socket.recv_from(&mut buffer) => {
                    match PDU::decode(&mut buffer.as_slice()).await {
                        Ok(pdu) => {
                            match sender.send(pdu) {
                                Ok(()) => {}
                                Err(error) => {
                                    error!("Transport found disconnect sending channel: {}", error);
                                    return Err(IoError::from(ErrorKind::ConnectionAborted));
                                }
                            };
                        }
                        Err(error) => {
                            error!("Error decoding PDU: {}", error);
                            // might need to stop depending on the error.
                            // some are recoverable though
                        }
                    }
                }
                Some((entity, pdu)) = recv.recv() => {
                    self.request(entity, pdu).await?

                },
                else => {
                    info!("UdpSocket and Channel disconnected");
                    break
                }
            };
            // match self.socket.recv_from(&mut buffer) {
            //     Ok(_n) => match PDU::decode(&mut buffer.as_slice()).await {
            //         Ok(pdu) => {
            //             match sender.send(pdu) {
            //                 Ok(()) => {}
            //                 Err(error) => {
            //                     error!("Transport found disconnect sending channel: {}", error);
            //                     return Err(IoError::from(ErrorKind::ConnectionAborted));
            //                 }
            //             };
            //         }
            //         Err(error) => {
            //             error!("Error decoding PDU: {}", error);
            //             // might need to stop depending on the error.
            //             // some are recoverable though
            //         }
            //     },
            //     Err(ref e)
            //         if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut =>
            //     {
            //         // continue to trying to send
            //     }
            //     Err(e) => {
            //         error!("encountered IO error: {e}");
            //         return Err(e);
            //     }
            // };
            // match recv.try_recv() {
            //     Ok((entity, pdu)) => s,
            //     Err(crossbeam_channel::TryRecvError::Empty) => {
            //         // nothing to do here
            //     }
            //     Err(err @ crossbeam_channel::TryRecvError::Disconnected) => {
            //         error!("Transport found disconnected channel: {}", err);
            //         return Err(IoError::from(ErrorKind::ConnectionAborted));
            //     }
            // };
            // thread::sleep(Duration::from_micros(500))
        }
        Ok(())
    }
}

#[cfg(feature = "uart")]
#[async_trait]
impl PDUTransport for SerialStream {
    fn is_ready(&self) -> bool {
        true
    }

    async fn request(&mut self, _destination: VariableID, pdu: PDU) -> Result<(), IoError> {
        self.write_all(pdu.encode().as_slice()).await
    }

    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: UnboundedSender<PDU>,
        mut recv: UnboundedReceiver<(VariableID, PDU)>,
    ) -> Result<(), IoError> {
        while !signal.load(Ordering::Relaxed) {
            // if there is anything in the read channel
            // read one PDU at a time
            // This gives a chance to send too without blocking
            // if incoming data is persistent
            tokio::select! {
                decode_result = PDU::decode(self), if self.bytes_to_read()? > 0 => match decode_result {
                    Ok(pdu) => {
                        match sender.send(pdu) {
                            Ok(()) => {}
                            Err(error) => {
                                error!("Transport found disconnect sending channel: {}", error);
                                return Err(IoError::from(ErrorKind::ConnectionAborted));
                            }
                        };
                    }
                    Err(error) => {
                        error!("Error decoding PDU: {}", error);
                        // might need to stop depending on the error.
                        // some are recoverable though
                    }
                },
                Some((_entity, pdu)) = recv.recv() => self.request(_entity, pdu).await?,
                else => break,
            };
        }

        Ok(())
    }
}
