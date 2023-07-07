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

use crate::cfdp_core::pdu::{PDUEncode, VariableID, PDU};

/// Transports are designed to run in a thread in the background
/// inside a [Daemon](crate::Daemon) process
#[async_trait]
pub trait PDUTransport {
    /// Look up the address of of the destination entity ID and send the PDU.
    /// Errors if the destination Entity does not have an associated address.
    async fn request(&mut self, destination: VariableID, pdu: PDU) -> Result<(), IoError>;

    /// Recieves a PDU from the associated communication protocol.
    async fn receive(&mut self) -> Result<PDU, IoError>;

    /// Provides logic for listening for incoming PDUs and sending any outbound PDUs
    /// A default implementeation is provided for convenience.
    ///
    /// This method relies on [tokio::select], as a result [request](PDUTransport::request) and [receive](PDUTransport::receive) must be cancel safe.
    ///
    /// A transport implementation will send any received messages through the
    /// [Sender] channel to the [Daemon](crate::Daemon).
    /// The [Receiver] channel is used to recv PDUs from the Daemon and send them to their respective remote Entity.
    /// The [Daemon](crate::Daemon) is responsible for receiving messages and distribute them to each
    /// transaction [Send](crate::transaction::SendTransaction) or [Recv](crate::transaction::RecvTransaction)
    /// The signal is used to indicate a shutdown operation was requested.
    async fn pdu_handler(
        &mut self,
        signal: Arc<AtomicBool>,
        sender: Sender<PDU>,
        mut recv: Receiver<(VariableID, PDU)>,
    ) -> Result<(), IoError> {
        while !signal.load(Ordering::Relaxed) {
            tokio::select! {
                pdu = self.receive() => {
                    match pdu{
                        Ok(pdu) => match sender.send(pdu).await {
                            Ok(()) => {}
                            Err(error) => {
                                error!("Channel to daemon severed: {}", error);
                                return Err(IoError::from(ErrorKind::ConnectionAborted));
                            }
                        },
                        Err(err) => {
                            error!("Error decoding PDU: {}", err);

                        }
                    };
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

/// A wrapper struct around a [UdpSocket] and a Mapping from
/// EntityIDs to [SocketAddr] instances.
pub struct UdpTransport {
    socket: UdpSocket,

    buffer: Vec<u8>,
    entity_map: HashMap<VariableID, SocketAddr>,
}
impl UdpTransport {
    pub async fn new<T: ToSocketAddrs + Debug>(
        addr: T,
        entity_map: HashMap<VariableID, SocketAddr>,
    ) -> Result<Self, IoError> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self {
            socket,
            buffer: vec![0_u8; u16::MAX as usize],
            entity_map,
        })
    }
}
impl TryFrom<(UdpSocket, HashMap<VariableID, SocketAddr>)> for UdpTransport {
    type Error = IoError;
    fn try_from(inputs: (UdpSocket, HashMap<VariableID, SocketAddr>)) -> Result<Self, Self::Error> {
        let me = Self {
            socket: inputs.0,
            // this buffer will be 511 KiB, should be sufficiently small;
            buffer: vec![0_u8; u16::MAX as usize],
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
        self.socket.send_to(pdu.encode().as_slice(), addr).await?;
        Ok(())
    }

    async fn receive(&mut self) -> Result<PDU, IoError> {
        let (_n, _addr) = self.socket.recv_from(&mut self.buffer).await?;

        match PDU::decode(&mut self.buffer.as_slice()) {
            Ok(pdu) => Ok(pdu),
            Err(err) => {
                // might need to stop depending on the error.
                // some are recoverable though
                Err(IoError::new(ErrorKind::InvalidData, err.to_string()))
            }
        }
    }
}
