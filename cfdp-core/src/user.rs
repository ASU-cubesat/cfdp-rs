use std::io::{Error as IoError, ErrorKind, Write};

use crate::{
    daemon::{PutRequest, UserPrimitive, SOCKET_ADDR},
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
        let mut connection = LocalSocketStream::connect(self.socket.as_str())?;
        connection.write_all(primitive.encode().as_slice())?;

        let id = (
            EntityID::decode(&mut connection).map_err(|_| IoError::from(ErrorKind::InvalidData))?,
            TransactionSeqNum::decode(&mut connection)
                .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
        );
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
    pub fn report(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Report(transaction.0, transaction.1))
    }
}
