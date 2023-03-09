mod common;
use common::*;

use cfdp_core::{
    daemon::PutRequest,
    pdu::{
        Condition, DeliveryCode, DirectoryListingRequest, EntityID, FileStatusCode,
        FileStoreAction, FileStoreRequest, MessageToUser, OriginatingTransactionIDMessage,
        PDUEncode, ProxyOperation, ProxyPutRequest, ProxyPutResponse, RemoteResumeRequest,
        TransactionSeqNum, TransmissionMode, UserOperation, UserRequest, UserResponse,
    },
    transaction::TransactionID,
};

use rstest::rstest;

#[rstest]
fn proxy_req(#[values(true, false)] use_mode: bool) {
    let origin_id = (EntityID::from(55_u16), TransactionSeqNum::from(12_u16));
    let mut messages = vec![
        ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
            action_code: FileStoreAction::CreateDirectory,
            first_filename: "/tmp".into(),
            second_filename: "".into(),
        }),
        ProxyOperation::ProxyPutRequest(ProxyPutRequest {
            destination_entity_id: EntityID::from(3_u16),
            source_filename: "test_file".into(),
            destination_filename: "out_file".into(),
        }),
        ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
            action_code: FileStoreAction::AppendFile,
            first_filename: "first_file".into(),
            second_filename: "second_file".into(),
        }),
        ProxyOperation::ProxyMessageToUser(MessageToUser {
            message_text: "help".as_bytes().to_vec(),
        }),
    ];

    if use_mode {
        messages.push(ProxyOperation::ProxyTransmissionMode(
            TransmissionMode::Acknowledged,
        ));
    }

    let recovered = get_proxy_request(&origin_id, messages.as_slice());

    let expected = PutRequest {
        source_filename: "test_file".into(),
        destination_filename: "out_file".into(),
        destination_entity_id: EntityID::from(3_u16),
        transmission_mode: if use_mode {
            TransmissionMode::Acknowledged
        } else {
            TransmissionMode::Unacknowledged
        },
        filestore_requests: vec![
            FileStoreRequest {
                action_code: FileStoreAction::CreateDirectory,
                first_filename: "/tmp".into(),
                second_filename: "".into(),
            },
            FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: "first_file".into(),
                second_filename: "second_file".into(),
            },
        ],
        message_to_user: vec![
            MessageToUser {
                message_text: "help".as_bytes().to_vec(),
            },
            MessageToUser {
                message_text: UserOperation::OriginatingTransactionIDMessage(
                    OriginatingTransactionIDMessage {
                        source_entity_id: origin_id.0,
                        transaction_sequence_number: origin_id.1,
                    },
                )
                .encode(),
            },
        ],
    };
    assert_eq!(1, recovered.len());
    assert_eq!(expected, recovered[0])
}

#[test]
fn categorize_user_message() {
    let origin_id = (EntityID::from(55_u16), TransactionSeqNum::from(12_u16));
    let proxy_ops = vec![
        ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
            action_code: FileStoreAction::CreateDirectory,
            first_filename: "/tmp".into(),
            second_filename: "".into(),
        }),
        ProxyOperation::ProxyPutRequest(ProxyPutRequest {
            destination_entity_id: EntityID::from(3_u16),
            source_filename: "test_file".into(),
            destination_filename: "out_file".into(),
        }),
        ProxyOperation::ProxyFileStoreRequest(FileStoreRequest {
            action_code: FileStoreAction::AppendFile,
            first_filename: "first_file".into(),
            second_filename: "second_file".into(),
        }),
        ProxyOperation::ProxyMessageToUser(MessageToUser {
            message_text: "help".as_bytes().to_vec(),
        }),
        ProxyOperation::ProxyTransmissionMode(TransmissionMode::Acknowledged),
    ];

    let put_requests = vec![PutRequest {
        source_filename: "test_file".into(),
        destination_filename: "out_file".into(),
        destination_entity_id: EntityID::from(3_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![
            FileStoreRequest {
                action_code: FileStoreAction::CreateDirectory,
                first_filename: "/tmp".into(),
                second_filename: "".into(),
            },
            FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: "first_file".into(),
                second_filename: "second_file".into(),
            },
        ],
        message_to_user: vec![
            MessageToUser {
                message_text: "help".as_bytes().to_vec(),
            },
            MessageToUser::from(UserOperation::OriginatingTransactionIDMessage(
                OriginatingTransactionIDMessage {
                    source_entity_id: EntityID::from(55_u16),
                    transaction_sequence_number: TransactionSeqNum::from(12_u16),
                },
            )),
        ],
    }];

    let requests = vec![
        UserRequest::DirectoryListing(DirectoryListingRequest {
            directory_name: "/home/do".into(),
            directory_filename: "/home/do.listing".into(),
        }),
        UserRequest::RemoteResume(RemoteResumeRequest {
            source_entity_id: EntityID::from(1_u16),
            transaction_sequence_number: TransactionSeqNum::from(2_u16),
        }),
    ];

    let responses = vec![UserResponse::ProxyPut(ProxyPutResponse {
        condition: Condition::FileChecksumFailure,
        delivery_code: DeliveryCode::Incomplete,
        file_status: FileStatusCode::Unreported,
    })];

    let other_message = vec![MessageToUser {
        message_text: "help".as_bytes().to_vec(),
    }];

    let cancel_id: TransactionID = (EntityID::from(16_u16), TransactionSeqNum::from(3_u32));

    let mut user_messages: Vec<MessageToUser> = proxy_ops
        .iter()
        .map(|msg| MessageToUser::from(UserOperation::ProxyOperation(msg.clone())))
        .chain(
            responses
                .iter()
                .map(|resp| MessageToUser::from(UserOperation::Response(resp.clone()))),
        )
        .chain(
            requests
                .iter()
                .map(|req| MessageToUser::from(UserOperation::Request(req.clone()))),
        )
        .chain(other_message.clone().into_iter())
        .collect();
    user_messages.extend(vec![
        MessageToUser::from(UserOperation::ProxyOperation(
            ProxyOperation::ProxyPutCancel,
        )),
        MessageToUser::from(UserOperation::OriginatingTransactionIDMessage(
            OriginatingTransactionIDMessage {
                source_entity_id: EntityID::from(16_u16),
                transaction_sequence_number: TransactionSeqNum::from(3_u32),
            },
        )),
    ]);

    let (proxy, req, resp, cancel, message) = categorize_user_msg(&origin_id, user_messages);

    assert_eq!(put_requests, proxy);
    assert_eq!(requests, req);
    assert_eq!(responses, resp);
    assert_eq!(cancel_id, cancel.unwrap());
    assert_eq!(other_message, message)
}
