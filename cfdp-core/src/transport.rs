use std::{
    collections::HashMap,
    fmt::Debug,
    io::{Error as IoError, ErrorKind},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use log::error;
use tokio::{
    net::{ToSocketAddrs, UdpSocket},
    sync::mpsc::{Receiver, Sender},
};

use crate::pdu::{PDUEncode, VariableID, PDU};

/// Transports are designed to run in a thread in the background
/// inside a [Daemon](crate::daemon::Daemon) process
#[async_trait]
pub trait PDUTransport {
    /// Send input PDU to the remote
    /// The implementation must have a method to lookup an Entity's address from the ID
    async fn request(&mut self, destination: VariableID, pdu: PDU) -> Result<(), IoError>;

    /// Provides logic for listening for incoming PDUs and sending any outbound PDUs

    /// A transport implementation will send any received messages through the
    /// [Sender] channel to the [Daemon](crate::daemon::Daemon).
    /// The [Receiver] channel is used to recv PDUs from the Daemon and send them to their respective remote Entity.
    /// The [Daemon](crate::daemon::Daemon) is responsible for receiving messages and distribute them to each
    /// transaction [Send](crate::transaction::SendTransaction) or [Recv](crate::transaction::RecvTransaction)
    /// The signal is used to indicate a shutdown operation was requested.
    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        mut recv: Receiver<(VariableID, PDU)>,
    ) -> Result<(), IoError>;
}

/// A wrapper struct around a [UdpSocket] and a Mapping from
/// EntityIDs to [SocketAddr] instances.
pub struct UdpTransport {
    socket: UdpSocket,
    entity_map: HashMap<VariableID, SocketAddr>,
}
impl UdpTransport {
    pub async fn new<T: ToSocketAddrs + Debug>(
        addr: T,
        entity_map: HashMap<VariableID, SocketAddr>,
    ) -> Result<Self, IoError> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self { socket, entity_map })
    }
}
impl TryFrom<(UdpSocket, HashMap<VariableID, SocketAddr>)> for UdpTransport {
    type Error = IoError;
    fn try_from(inputs: (UdpSocket, HashMap<VariableID, SocketAddr>)) -> Result<Self, Self::Error> {
        let me = Self {
            socket: inputs.0,
            entity_map: inputs.1,
        };
        Ok(me)
    }
}

#[async_trait]
impl PDUTransport for UdpTransport {
    async fn request(&mut self, destination: VariableID, pdu: PDU) -> Result<(), IoError> {
        let addr = self
            .entity_map
            .get(&destination)
            .ok_or_else(|| IoError::from(ErrorKind::AddrNotAvailable))?;
        self.socket
            .send_to(pdu.encode().as_slice(), addr)
            .await
            .map(|_n| ())?;
        Ok(())
    }

    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        mut recv: Receiver<(VariableID, PDU)>,
    ) -> Result<(), IoError> {
        // this buffer will be 511 KiB, should be sufficiently small;
        let mut buffer = vec![0_u8; u16::MAX as usize];
        while !signal.load(Ordering::Relaxed) {
            tokio::select! {
                Ok((_n, _addr)) = self.socket.recv_from(&mut buffer) => {
                    match PDU::decode(&mut buffer.as_slice()) {
                        Ok(pdu) => {
                            match sender.send(pdu).await {
                                Ok(()) => {}
                                Err(error) => {
                                    error!("Channel to daemon severed: {}", error);
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
                },
                Some((entity, pdu)) = recv.recv() => {
                    self.request(entity, pdu).await?;
                },
                else => {
                    log::info!("UdpSocket or Channel disconnected");
                    break
                }
            }
            // this should be at minimum made configurable
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
        Ok(())
    }
}
