use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{Error as IoError, ErrorKind, Write},
    marker::PhantomData,
    net::{SocketAddr, ToSocketAddrs, UdpSocket},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use camino::Utf8PathBuf;
use cfdp_core::{
    daemon::{
        Daemon, EntityConfig, FinishedIndication, Indication, MetadataRecvIndication, NakProcedure,
        PutRequest, Report, UserPrimitive,
    },
    filestore::{ChecksumType, FileStore, NativeFileStore},
    pdu::{
        CRCFlag, Condition, DirectoryListingResponse, EntityID, FaultHandlerAction,
        ListingResponseCode, MessageToUser, OriginatingTransactionIDMessage, PDUDirective,
        PDUEncode, PDUPayload, ProxyOperation, ProxyPutRequest, ProxyPutResponse,
        RemoteStatusReportResponse, RemoteSuspendResponse, TransactionSeqNum, TransactionStatus,
        TransmissionMode, UserOperation, UserRequest, UserResponse, VariableID, PDU,
    },
    transaction::TransactionID,
    transport::{PDUTransport, UdpTransport},
};
use crossbeam_channel::{bounded, unbounded, Receiver, Sender};
use itertools::{Either, Itertools};
use log::{error, info};
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

pub(crate) fn get_proxy_request(
    origin_id: &TransactionID,
    messages: &[ProxyOperation],
) -> Vec<PutRequest> {
    let mut out = vec![];
    let proxy_puts: Vec<ProxyPutRequest> = messages
        .iter()
        .filter_map(|msg| match msg {
            ProxyOperation::ProxyPutRequest(req) => Some(req.clone()),
            _ => None,
        })
        .collect();

    for put in proxy_puts {
        let transmission_mode = messages
            .iter()
            .find_map(|msg| match msg {
                ProxyOperation::ProxyTransmissionMode(mode) => Some(*mode),
                _ => None,
            })
            .unwrap_or(TransmissionMode::Unacknowledged);

        let filestore_requests = messages
            .iter()
            .filter_map(|msg| match msg {
                ProxyOperation::ProxyFileStoreRequest(req) => Some(req.clone()),
                _ => None,
            })
            .collect();

        let mut message_to_user: Vec<MessageToUser> = messages
            .iter()
            .filter_map(|msg| match msg {
                ProxyOperation::ProxyMessageToUser(req) => Some(req.clone()),
                _ => None,
            })
            .collect();

        // Should include an originating TransactionIDMessage
        // But if the implementation doesn't let's not worry about it too much
        message_to_user.push(MessageToUser::from(
            UserOperation::OriginatingTransactionIDMessage(OriginatingTransactionIDMessage {
                source_entity_id: origin_id.0,
                transaction_sequence_number: origin_id.1,
            }),
        ));

        let req = PutRequest {
            source_filename: put.source_filename,
            destination_filename: put.destination_filename,
            destination_entity_id: put.destination_entity_id,
            transmission_mode,
            filestore_requests,
            message_to_user,
        };
        out.push(req)
    }

    out
}

type UserMessageCategories = (
    // proxy operations
    Vec<PutRequest>,
    // user requests
    Vec<UserRequest>,
    // responses to log
    Vec<UserResponse>,
    // Transaction ID to cancel
    Option<TransactionID>,
    // Others
    Vec<MessageToUser>,
);

pub(crate) fn categorize_user_msg(
    origin_id: &TransactionID,
    messages: Vec<MessageToUser>,
) -> UserMessageCategories {
    let (user_ops, other_messages): (Vec<UserOperation>, Vec<MessageToUser>) =
        messages.into_iter().partition_map(|msg| {
            match UserOperation::decode(&mut msg.message_text.as_slice()) {
                Ok(operation) => Either::Left(operation),
                Err(_) => Either::Right(msg),
            }
        });

    let cancel_id = user_ops
        .iter()
        .find(|&msg| msg == &UserOperation::ProxyOperation(ProxyOperation::ProxyPutCancel))
        .and_then(|_| {
            user_ops.iter().find_map(|msg| {
                if let UserOperation::OriginatingTransactionIDMessage(origin) = msg {
                    Some(TransactionID(
                        origin.source_entity_id,
                        origin.transaction_sequence_number,
                    ))
                } else {
                    None
                }
            })
        });
    let proxy_ops: Vec<ProxyOperation> = user_ops
        .iter()
        .filter_map(|req| {
            if let UserOperation::ProxyOperation(op) = req {
                Some(op.clone())
            } else {
                None
            }
        })
        .collect();

    let other_reqs = user_ops
        .iter()
        .filter_map(|req| {
            if let UserOperation::Request(request) = req {
                Some(request.clone())
            } else {
                None
            }
        })
        .collect();

    let responses = user_ops
        .iter()
        .filter_map(|req| {
            if let UserOperation::Response(response) = req {
                Some(response.clone())
            } else {
                None
            }
        })
        .collect();
    let proxy_reqs = get_proxy_request(origin_id, proxy_ops.as_slice());
    (proxy_reqs, other_reqs, responses, cancel_id, other_messages)
}

type UserSplit = (TestUserHalf, Receiver<UserPrimitive>, Sender<Indication>);

pub(crate) struct TestUser {
    internal_tx: Sender<UserPrimitive>,
    internal_rx: Receiver<UserPrimitive>,
    // channel for daemon to indicate a finished transaction
    indication_tx: Sender<Indication>,
    // Indication listener thread
    indication_handle: JoinHandle<()>,
    history: Arc<RwLock<HashMap<TransactionID, Report>>>,
}
impl TestUser {
    pub(crate) fn new<T: FileStore + Send + Sync + 'static>(filestore: Arc<T>) -> Self {
        let (internal_tx, internal_rx) = bounded::<UserPrimitive>(1);
        let (indication_tx, indication_rx) = unbounded::<Indication>();
        let history = Arc::new(RwLock::new(HashMap::<TransactionID, Report>::new()));

        let auto_history = history.clone();
        let auto_sender = internal_tx.clone();

        let indication_handle = thread::spawn(move || {
            let mut proxy_map = HashMap::new();
            while let Ok(indication) = indication_rx.recv() {
                // (origin_id, tx_mode, messages)
                match indication {
                    Indication::MetadataRecv(MetadataRecvIndication {
                        id: origin_id,
                        transmission_mode: tx_mode,
                        user_messages: messages,
                        ..
                    }) => {
                        let (put_requests, user_reqs, responses, cancel_id, other_messages) =
                            categorize_user_msg(&origin_id, messages);

                        for request in put_requests {
                            let (put_sender, put_recv) = bounded(1);

                            auto_sender
                                .send(UserPrimitive::Put(request, put_sender))
                                .expect("Unable to send auto request");

                            let id = put_recv.recv().expect("Recv channel disconnected: ");
                            proxy_map.insert(id, origin_id);
                        }

                        if let Some(id) = cancel_id {
                            let primitive = UserPrimitive::Cancel(id);
                            auto_sender
                                .send(primitive)
                                .map_err(|_| {
                                    IoError::new(
                                        ErrorKind::ConnectionReset,
                                        "Daemon Half of User disconnected.",
                                    )
                                })
                                .expect("Auto sender error ");
                        }

                        for req in user_reqs.into_iter() {
                            let request = match req {
                                UserRequest::DirectoryListing(directory_request) => match filestore
                                    .list_directory(&directory_request.directory_name)
                                {
                                    Ok(listing) => {
                                        let outfile = directory_request
                                            .directory_name
                                            .as_path()
                                            .with_extension(".listing");

                                        let response_code = match filestore
                                            .open(
                                                &outfile,
                                                OpenOptions::new()
                                                    .create(true)
                                                    .truncate(true)
                                                    .write(true),
                                            )
                                            .map(|mut handle| handle.write_all(listing.as_bytes()))
                                        {
                                            Ok(Ok(())) => ListingResponseCode::Successful,
                                            _ => ListingResponseCode::Unsuccessful,
                                        };
                                        PutRequest {
                                            source_filename: outfile,
                                            destination_filename: directory_request
                                                .directory_filename
                                                .clone(),
                                            destination_entity_id: origin_id.0,
                                            transmission_mode: tx_mode,
                                            filestore_requests: vec![],
                                            message_to_user: vec![
                                                MessageToUser::from(
                                                    UserOperation::OriginatingTransactionIDMessage(
                                                        OriginatingTransactionIDMessage {
                                                            source_entity_id: origin_id.0,
                                                            transaction_sequence_number: origin_id
                                                                .1,
                                                        },
                                                    ),
                                                ),
                                                MessageToUser::from(UserOperation::Response(
                                                    UserResponse::DirectoryListing(
                                                        DirectoryListingResponse {
                                                            response_code,
                                                            directory_name: directory_request
                                                                .directory_name,
                                                            directory_filename: directory_request
                                                                .directory_filename,
                                                        },
                                                    ),
                                                )),
                                            ],
                                        }
                                    }
                                    Err(_) => PutRequest {
                                        source_filename: "".into(),
                                        destination_filename: "".into(),
                                        destination_entity_id: origin_id.0,
                                        transmission_mode: tx_mode,
                                        filestore_requests: vec![],
                                        message_to_user: vec![
                                            MessageToUser::from(
                                                UserOperation::OriginatingTransactionIDMessage(
                                                    OriginatingTransactionIDMessage {
                                                        source_entity_id: origin_id.0,
                                                        transaction_sequence_number: origin_id.1,
                                                    },
                                                ),
                                            ),
                                            MessageToUser::from(UserOperation::Response(
                                                UserResponse::DirectoryListing(
                                                    DirectoryListingResponse {
                                                        response_code:
                                                            ListingResponseCode::Unsuccessful,
                                                        directory_name: directory_request
                                                            .directory_name,
                                                        directory_filename: directory_request
                                                            .directory_filename,
                                                    },
                                                ),
                                            )),
                                        ],
                                    },
                                },
                                UserRequest::RemoteStatusReport(report_request) => {
                                    let (report_tx, report_rx) = bounded(0);
                                    let id = TransactionID(
                                        report_request.source_entity_id,
                                        report_request.transaction_sequence_number,
                                    );
                                    let primitive = UserPrimitive::Report(id, report_tx);

                                    auto_sender
                                        .send(primitive)
                                        .map_err(|_| {
                                            IoError::new(
                                                ErrorKind::ConnectionReset,
                                                "Daemon Half of User disconnected.",
                                            )
                                        })
                                        .expect("error asking for report.");

                                    let report = match report_rx.recv().map_err(|_| {
                                        IoError::new(
                                            ErrorKind::ConnectionReset,
                                            "Daemon Half of User disconnected.",
                                        )
                                    }) {
                                        Ok(report) => Some(report),
                                        // try to get from history here
                                        Err(_) => auto_history.read().unwrap().get(&id).cloned(),
                                    };
                                    let response = {
                                        match report {
                                            Some(data) => RemoteStatusReportResponse {
                                                transaction_status: data.status,
                                                source_entity_id: data.id.0,
                                                transaction_sequence_number: data.id.1,
                                                response_code: true,
                                            },
                                            None => RemoteStatusReportResponse {
                                                transaction_status: TransactionStatus::Unrecognized,
                                                source_entity_id: report_request.source_entity_id,
                                                transaction_sequence_number: report_request
                                                    .transaction_sequence_number,
                                                response_code: false,
                                            },
                                        }
                                    };
                                    PutRequest {
                                        source_filename: "".into(),
                                        destination_filename: "".into(),
                                        destination_entity_id: origin_id.0,
                                        transmission_mode: tx_mode,
                                        filestore_requests: vec![],
                                        message_to_user: vec![
                                            MessageToUser::from(
                                                UserOperation::OriginatingTransactionIDMessage(
                                                    OriginatingTransactionIDMessage {
                                                        source_entity_id: origin_id.0,
                                                        transaction_sequence_number: origin_id.1,
                                                    },
                                                ),
                                            ),
                                            MessageToUser::from(UserOperation::Response(
                                                UserResponse::RemoteStatusReport(response),
                                            )),
                                        ],
                                    }
                                }
                                UserRequest::RemoteSuspend(suspend_req) => {
                                    let primitive = UserPrimitive::Suspend(TransactionID(
                                        suspend_req.source_entity_id,
                                        suspend_req.transaction_sequence_number,
                                    ));

                                    let suspend_indication = auto_sender
                                        .send(primitive)
                                        .map_err(|_| {
                                            IoError::new(
                                                ErrorKind::ConnectionReset,
                                                "Daemon Half of User disconnected.",
                                            )
                                        })
                                        .is_ok();

                                    PutRequest {
                                        source_filename: "".into(),
                                        destination_filename: "".into(),
                                        destination_entity_id: origin_id.0,
                                        transmission_mode: tx_mode,
                                        filestore_requests: vec![],
                                        message_to_user: vec![
                                            MessageToUser::from(
                                                UserOperation::OriginatingTransactionIDMessage(
                                                    OriginatingTransactionIDMessage {
                                                        source_entity_id: origin_id.0,
                                                        transaction_sequence_number: origin_id.1,
                                                    },
                                                ),
                                            ),
                                            MessageToUser::from(UserOperation::Response(
                                                UserResponse::RemoteSuspend(
                                                    RemoteSuspendResponse {
                                                        suspend_indication,
                                                        transaction_status:
                                                            TransactionStatus::Unrecognized,
                                                        source_entity_id: suspend_req
                                                            .source_entity_id,
                                                        transaction_sequence_number: suspend_req
                                                            .transaction_sequence_number,
                                                    },
                                                ),
                                            )),
                                        ],
                                    }
                                }
                                UserRequest::RemoteResume(resume_request) => {
                                    let primitive = UserPrimitive::Resume(TransactionID(
                                        resume_request.source_entity_id,
                                        resume_request.transaction_sequence_number,
                                    ));

                                    let suspend_indication = auto_sender
                                        .send(primitive)
                                        .map_err(|_| {
                                            IoError::new(
                                                ErrorKind::ConnectionReset,
                                                "Daemon Half of User disconnected.",
                                            )
                                        })
                                        .is_ok();

                                    PutRequest {
                                        source_filename: "".into(),
                                        destination_filename: "".into(),
                                        destination_entity_id: origin_id.0,
                                        transmission_mode: tx_mode,
                                        filestore_requests: vec![],
                                        message_to_user: vec![
                                            MessageToUser::from(
                                                UserOperation::OriginatingTransactionIDMessage(
                                                    OriginatingTransactionIDMessage {
                                                        source_entity_id: origin_id.0,
                                                        transaction_sequence_number: origin_id.1,
                                                    },
                                                ),
                                            ),
                                            MessageToUser::from(UserOperation::Response(
                                                UserResponse::RemoteSuspend(
                                                    RemoteSuspendResponse {
                                                        suspend_indication,
                                                        transaction_status:
                                                            TransactionStatus::Unrecognized,
                                                        source_entity_id: resume_request
                                                            .source_entity_id,
                                                        transaction_sequence_number: resume_request
                                                            .transaction_sequence_number,
                                                    },
                                                ),
                                            )),
                                        ],
                                    }
                                }
                            };
                            let (sender, _recv) = bounded(0);
                            auto_sender
                                .send(UserPrimitive::Put(request, sender))
                                .expect("Unable to send auto request");
                        }
                        for response in responses {
                            // log indication of the response received!
                            info!("Received User Operation Response: {:?}", response);
                        }
                        for message in other_messages {
                            // also log this!
                            info!("Received Messages I can't decifer {:?}", message);
                        }
                    }
                    Indication::Finished(FinishedIndication {
                        id,
                        report,
                        file_status,
                        delivery_code,
                        filestore_responses,
                    }) => {
                        if let Some(origin) = proxy_map.get(&id) {
                            let mut message_to_user = vec![
                                MessageToUser::from(UserOperation::Response(
                                    UserResponse::ProxyPut(ProxyPutResponse {
                                        condition: report.condition,
                                        delivery_code,
                                        file_status,
                                    }),
                                )),
                                MessageToUser::from(
                                    UserOperation::OriginatingTransactionIDMessage(
                                        OriginatingTransactionIDMessage {
                                            source_entity_id: origin.0,
                                            transaction_sequence_number: origin.1,
                                        },
                                    ),
                                ),
                            ];
                            filestore_responses.iter().for_each(|res| {
                                message_to_user.push(MessageToUser::from(UserOperation::Response(
                                    UserResponse::ProxyFileStore(res.clone()),
                                )))
                            });

                            let req = PutRequest {
                                source_filename: "".into(),
                                destination_filename: "".into(),
                                destination_entity_id: origin.0,
                                transmission_mode: TransmissionMode::Unacknowledged,
                                filestore_requests: vec![],
                                message_to_user,
                            };
                            // we should be able to connect to the socket we are running
                            // just fine. but we can ignore errors per
                            // CCSDS 727.0-B-5  § 6.2.5.1.2
                            let (sender, _) = bounded(0);
                            let _ =
                                auto_sender
                                    .send(UserPrimitive::Put(req, sender))
                                    .map_err(|_| {
                                        IoError::new(
                                            ErrorKind::ConnectionReset,
                                            "Daemon Half of User disconnected.",
                                        )
                                    });
                        }
                    }
                    Indication::Report(report) => {
                        auto_history.write().unwrap().insert(report.id, report);
                    }
                    // ignore everything else for now.
                    _ => {}
                };
            }
        });

        Self {
            internal_tx,
            internal_rx,
            indication_tx,
            indication_handle,
            history,
        }
    }

    pub(crate) fn split(self) -> UserSplit {
        let TestUser {
            internal_tx,
            internal_rx,
            indication_tx,
            indication_handle,
            history,
        } = self;
        (
            TestUserHalf {
                internal_tx,
                _indication_handle: indication_handle,
                history,
            },
            internal_rx,
            indication_tx,
        )
    }
}

// useful for spawning proxy responses. need to listen for completion indications.
// {
//     let mut message_to_user = vec![
//         MessageToUser::from(UserOperation::Response(UserResponse::ProxyPut(
//             self.get_proxy_response(),
//         ))),
//         MessageToUser::from(UserOperation::OriginatingTransactionIDMessage(
//             origin.clone(),
//         )),
//     ];
//     finished.filestore_response.iter().for_each(|res| {
//         message_to_user.push(MessageToUser::from(UserOperation::Response(
//             UserResponse::ProxyFileStore(res.clone()),
//         )))
//     });

//     let req = PutRequest {
//         source_filename: "".into(),
//         destination_filename: "".into(),
//         destination_entity_id: origin.source_entity_id,
//         transmission_mode: TransmissionMode::Unacknowledged,
//         filestore_requests: vec![],
//         message_to_user,
//     };
//     // we should be able to connect to the socket we are running
//     // just fine. but we can ignore errors per
//     // CCSDS 727.0-B-5  § 6.2.5.1.2
//     let (sender, _) = bounded(0);
//     let _ = self.primitive_tx.send_timeout((UserPrimitive::Put(req), sender), Duration::from_millis(100));
// }

#[derive(Debug)]
pub struct TestUserHalf {
    internal_tx: Sender<UserPrimitive>,
    _indication_handle: JoinHandle<()>,
    history: Arc<RwLock<HashMap<TransactionID, Report>>>,
}
impl TestUserHalf {
    #[allow(unused)]
    pub fn put(&self, request: PutRequest) -> Result<TransactionID, IoError> {
        let (put_send, put_recv) = bounded(1);
        let primitive = UserPrimitive::Put(request, put_send);

        self.internal_tx.send(primitive).map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })?;
        put_recv.recv().map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })
    }

    // this function is actually used in series_f1 but series_f2 and f3 generate an unused warning
    // apparently related https://github.com/rust-lang/rust/issues/46379
    #[allow(unused)]
    pub fn cancel(&self, transaction: TransactionID) -> Result<(), IoError> {
        let primitive = UserPrimitive::Cancel(transaction);
        self.internal_tx.send(primitive).map_err(|_| {
            IoError::new(
                ErrorKind::ConnectionReset,
                "Daemon Half of User disconnected.",
            )
        })
    }

    #[allow(unused)]
    pub fn report(&self, transaction: TransactionID) -> Result<Option<Report>, IoError> {
        let (report_tx, report_rx) = bounded(1);
        let primitive = UserPrimitive::Report(transaction, report_tx);

        self.internal_tx.send(primitive).map_err(|err| {
            IoError::new(
                ErrorKind::ConnectionReset,
                format!("Daemon Half of User disconnected on send: {err}"),
            )
        })?;
        let response = match report_rx.recv() {
            Ok(report) => Some(report),
            // if the channel disconnects because the transaction is finished then just get from history.
            Err(_) => self.history.read().unwrap().get(&transaction).cloned(),
        };
        Ok(response)
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
        nak_procedure: NakProcedure::Deferred,
    };

    let remote_config = HashMap::from([
        (EntityID::from(0_u16), config.clone()),
        (EntityID::from(1_u16), config.clone()),
    ]);

    let local_filestore = filestore.clone();

    let local_user = TestUser::new(local_filestore.clone());
    let (local_userhalf, local_daemonhalf, indication_tx) = local_user.split();

    let mut local_daemon = Daemon::new(
        EntityID::from(0_u16),
        TransactionSeqNum::from(0_u16),
        local_transport_map,
        local_filestore,
        remote_config.clone(),
        config.clone(),
        signal.clone(),
        local_daemonhalf,
        indication_tx,
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

    let remote_filestore = filestore;
    let remote_user = TestUser::new(remote_filestore.clone());
    let (remote_userhalf, remote_daemonhalf, remote_indication_tx) = remote_user.split();

    let mut remote_daemon = Daemon::new(
        EntityID::from(1_u16),
        TransactionSeqNum::from(0_u16),
        remote_transport_map,
        remote_filestore,
        remote_config,
        config,
        signal.clone(),
        remote_daemonhalf,
        remote_indication_tx,
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

pub(crate) type UsersAndFilestore = (
    &'static TestUserHalf,
    &'static TestUserHalf,
    Arc<NativeFileStore>,
);
#[fixture]
#[once]
pub(crate) fn get_filestore(make_entities: &'static EntityConstructorReturn) -> UsersAndFilestore {
    (&make_entities.0, &make_entities.1, make_entities.2.clone())
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
                        continue;
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
                Ok((entity, pdu)) => {
                    self.request(entity, pdu)?;
                    continue;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => {
                    // nothing to do here
                }
                Err(err @ crossbeam_channel::TryRecvError::Disconnected) => {
                    error!("Transport found disconnected channel: {err}");
                    return Err(IoError::from(ErrorKind::ConnectionAborted));
                }
            };
            thread::sleep(Duration::from_millis(10))
        }
        Ok(())
    }
}
