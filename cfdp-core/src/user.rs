use std::io::{Error as IoError, ErrorKind};

use crate::{
    daemon::{PutRequest, Report, UserPrimitive, SOCKET_ADDR},
    pdu::{EntityID, PDUEncode, TransactionSeqNum},
    transaction::TransactionID,
};

use camino::Utf8PathBuf;
use interprocess::local_socket::tokio::LocalSocketStream;

use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    runtime::Runtime,
};
use tokio_util::compat::FuturesAsyncReadCompatExt;

pub struct BlockingUser {
    socket: Utf8PathBuf,
    /// A `current_thread` runtime for executing operations on the
    /// asynchronous client in a blocking manner.
    rt: Runtime,
}
impl BlockingUser {
    pub fn new(socket_address: Option<&str>) -> Result<Self, IoError> {
        let socket = socket_address.unwrap_or(SOCKET_ADDR);
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        Ok(Self {
            socket: Utf8PathBuf::from(socket),
            rt,
        })
    }
    fn send(&mut self, primitive: UserPrimitive) -> Result<(), IoError> {
        self.rt.block_on(async {
            let mut connection = LocalSocketStream::connect(self.socket.as_str())
                .await?
                .compat();
            connection.write_all(primitive.encode().as_slice()).await
        })
    }

    pub fn put(&mut self, request: PutRequest) -> Result<TransactionID, IoError> {
        let primitive = UserPrimitive::Put(request);

        self.rt.block_on(async {
            let mut connection = LocalSocketStream::connect(self.socket.as_str())
                .await?
                .compat();
            connection.write_all(primitive.encode().as_slice()).await?;
            Ok((
                EntityID::decode(&mut connection)
                    .await
                    .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
                TransactionSeqNum::decode(&mut connection)
                    .await
                    .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
            ))
        })
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

        let report = self.rt.block_on(async {
            let mut connection = LocalSocketStream::connect(self.socket.as_str())
                .await
                .ok()?
                .compat();

            connection
                .write_all(primitive.encode().as_slice())
                .await
                .ok()?;

            {
                match connection.read_u64().await.ok()? {
                    0 => None,
                    _ => Some(
                        Report::decode(&mut connection)
                            .await
                            .map_err(|_| IoError::from(ErrorKind::InvalidData))
                            .ok()?,
                    ),
                }
            }
        });
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

pub struct User {
    socket: Utf8PathBuf,
}
impl User {
    pub async fn new(socket_address: Option<&str>) -> Result<Self, IoError> {
        let socket = socket_address.unwrap_or(SOCKET_ADDR);
        Ok(Self {
            socket: Utf8PathBuf::from(socket),
        })
    }
    async fn send(&mut self, primitive: UserPrimitive) -> Result<(), IoError> {
        let mut connection = {
            'connector: loop {
                tokio::select! {
                    Ok(conn) = LocalSocketStream::connect(self.socket.as_str()) => {
                        break 'connector conn;
                    }
                }
            }
        }
        .compat();
        connection.write_all(primitive.encode().as_slice()).await
    }

    pub async fn put(&mut self, request: PutRequest) -> Result<TransactionID, IoError> {
        let primitive = UserPrimitive::Put(request);

        let mut connection = {
            'connector: loop {
                tokio::select! {
                    Ok(conn) = LocalSocketStream::connect(self.socket.as_str()) => {
                        break 'connector conn;
                    }
                }
            }
        }
        .compat();
        connection.write_all(primitive.encode().as_slice()).await?;

        Ok((
            EntityID::decode(&mut connection)
                .await
                .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
            TransactionSeqNum::decode(&mut connection)
                .await
                .map_err(|_| IoError::from(ErrorKind::InvalidData))?,
        ))
    }

    pub async fn suspend(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Suspend(transaction.0, transaction.1))
            .await
    }

    pub async fn resume(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Resume(transaction.0, transaction.1))
            .await
    }

    pub async fn cancel(&mut self, transaction: TransactionID) -> Result<(), IoError> {
        self.send(UserPrimitive::Cancel(transaction.0, transaction.1))
            .await
    }
    pub async fn report(&mut self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
        let primitive = UserPrimitive::Report(transaction.0, transaction.1);

        let report = {
            let mut connection = {
                'connector: loop {
                    tokio::select! {
                        Ok(conn) = LocalSocketStream::connect(self.socket.as_str()) => {
                            break 'connector conn;
                        }
                    }
                }
            }
            .compat();

            connection.write_all(primitive.encode().as_slice()).await?;

            {
                match connection.read_u64().await? {
                    0 => None,
                    _ => Some(
                        Report::decode(&mut connection)
                            .await
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
