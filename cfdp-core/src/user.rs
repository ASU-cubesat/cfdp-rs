use std::io::{Error as IoError, ErrorKind, Read, Write};

use crate::{
    daemon::{PutRequest, Report, UserPrimitive, SOCKET_ADDR},
    pdu::{EntityID, PDUEncode, TransactionSeqNum},
    transaction::TransactionID,
};

use camino::Utf8PathBuf;
use interprocess::local_socket::LocalSocketStream;

pub struct User {
    socket: Utf8PathBuf,
}
impl User {
    pub fn new(socket_address: Option<&str>) -> Result<Self, IoError> {
        let socket = socket_address.unwrap_or(SOCKET_ADDR);
        Ok(Self {
            socket: Utf8PathBuf::from(socket),
        })
    }
    fn send(&mut self, primitive: UserPrimitive) -> Result<(), IoError> {
        let mut connection = LocalSocketStream::connect(self.socket.as_str())?;
        connection.write_all(primitive.encode().as_slice())
    }

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

    pub fn suspend(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Suspend(transaction.0, transaction.1))
    }

    pub fn resume(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Resume(transaction.0, transaction.1))
    }

    pub fn cancel(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Cancel(transaction.0, transaction.1))
    }
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
