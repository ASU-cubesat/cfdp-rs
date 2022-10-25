use std::{fs::OpenOptions, sync::Arc, thread, time::Duration};

use camino::{Utf8Path, Utf8PathBuf};
use cfdp_core::{
    daemon::PutRequest,
    filestore::{ChecksumType, FileChecksum, FileStore, NativeFileStore},
    pdu::{
        DirectoryListingRequest, EntityID, FileStoreAction, FileStoreRequest, MessageToUser,
        ProxyOperation, ProxyPutRequest, TransmissionMode, UserOperation, UserRequest,
    },
    transaction::TransactionState,
    user::User,
};
use rstest::rstest;

mod common;
use common::get_filestore;

// Series F3
// Sequence 1 Test
// Test goal:
//  - Check 2-party Remote Put (Get)
// Configuration:
//  - Acknowledged
//  - File Size: Medium
//  - Execute remote put from remote. File should exist on remote
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(10))]
fn f3s01(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user_remote = User::new(Some(
        Utf8Path::new(local_path)
            .parent()
            .unwrap()
            .join("cfdp_remote.socket")
            .as_str(),
    ))
    .expect("Cannot connect to remote user.");

    let out_file: Utf8PathBuf = "remote/small_f3s1.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user_remote
        .put(PutRequest {
            source_filename: "".into(),
            destination_filename: "".into(),
            destination_entity_id: EntityID::from(0_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![],
            message_to_user: vec![
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyPutRequest(ProxyPutRequest {
                        destination_entity_id: EntityID::from(1_u16),
                        source_filename: "/local/medium.txt".into(),
                        destination_filename: out_file,
                    }),
                )),
                MessageToUser::from(UserOperation::ProxyOperation(
                    ProxyOperation::ProxyTransmissionMode(TransmissionMode::Acknowledged),
                )),
            ],
        })
        .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists())
}

// Series F3
// Sequence 2 Test
// Test goal:
//  - Check CreateFile directive
// Configuration:
//  - Acknowledged
//  - Create New file on remote
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s02(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/f3s2.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::CreateFile,
            first_filename: out_file,
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists())
}

// Series F3
// Sequence 3 Test
// Test goal:
//  - Check DeleteFile directive
// Configuration:
//  - Acknowledged
//  - Create New file on remote, then delete it with another transaction
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s03(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/f3s3.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::CreateFile,
            first_filename: out_file.clone(),
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists());

    let id = user
        .put(PutRequest {
            source_filename: "".into(),
            destination_filename: "".into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::DeleteFile,
                first_filename: out_file,
                second_filename: "".into(),
            }],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");
    while user
        .report(id.clone())
        .expect("Unable to obtain report.")
        .unwrap()
        .state
        != TransactionState::Terminated
    {
        thread::sleep(Duration::from_millis(5))
    }
    assert!(!path_to_out.exists());
}

// Series F3
// Sequence 4 Test
// Test goal:
//  - Check RenameFile directive
// Configuration:
//  - Acknowledged
//  - Create New file on remote, then Rename it in another transaction
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s04(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/f3s4.txt".into();
    let new_file: Utf8PathBuf = "remote/f3s4_new.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);
    let path_to_new = filestore.get_native_path(&new_file);

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::CreateFile,
            first_filename: out_file.clone(),
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists());

    let id = user
        .put(PutRequest {
            source_filename: "".into(),
            destination_filename: "".into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::RenameFile,
                first_filename: out_file,
                second_filename: new_file,
            }],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");
    while user
        .report(id.clone())
        .expect("Unable to obtain report.")
        .unwrap()
        .state
        != TransactionState::Terminated
    {
        thread::sleep(Duration::from_millis(5))
    }
    assert!(!path_to_out.exists());
    assert!(path_to_new.exists())
}

// Series F3
// Sequence 5 Test
// Test goal:
//  - Check AppendFile directive
// Configuration:
//  - Acknowledged
//  - Create blank file at remote
//  - Transfer M file
//  - Append new file to first file
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s05(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/f3s5.txt".into();
    let new_file: Utf8PathBuf = "remote/medium_f3s5.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);
    let path_to_new = filestore.get_native_path(&new_file);

    let id = user
        .put(PutRequest {
            source_filename: "/local/medium.txt".into(),
            destination_filename: new_file.clone(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::CreateFile,
                first_filename: out_file.clone(),
                second_filename: "".into(),
            }],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");

    while user
        .report(id.clone())
        .expect("unable to get report.")
        .is_none()
    {
        thread::sleep(Duration::from_millis(150))
    }
    let mut report = user
        .report(id.clone())
        .expect("unable to get report.")
        .unwrap();
    while report.state != TransactionState::Terminated {
        thread::sleep(Duration::from_millis(150));
        report = user
            .report(id.clone())
            .expect("unable to get report.")
            .unwrap();
    }

    println!("{report:?}");
    assert!(path_to_out.exists());
    assert!(path_to_new.exists());

    let id = user
        .put(PutRequest {
            source_filename: "".into(),
            destination_filename: "".into(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::AppendFile,
                first_filename: out_file.clone(),
                second_filename: new_file.clone(),
            }],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");
    while user
        .report(id.clone())
        .expect("Unable to obtain report.")
        .unwrap()
        .state
        != TransactionState::Terminated
    {
        thread::sleep(Duration::from_millis(5))
    }

    let checksum1 = filestore
        .open(out_file, OpenOptions::new().read(true))
        .expect("Unable to open file.")
        .checksum(ChecksumType::Modular)
        .expect("Error calculating checksum.");

    let checksum2 = filestore
        .open(new_file, OpenOptions::new().read(true))
        .expect("Unable to open file.")
        .checksum(ChecksumType::Modular)
        .expect("Error calculating checksum.");
    assert_eq!(checksum1, checksum2)
}

// Series F3
// Sequence 6 Test
// Test goal:
//  - Check ReplaceFile directive
// Configuration:
//  - Acknowledged
//  - Send small file to remote
//  - Transfer M file
//  - Replace small file with medium file
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s06(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/f3s6.txt".into();
    let new_file: Utf8PathBuf = "remote/medium_f3s6.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);
    let path_to_new = filestore.get_native_path(&new_file);

    user.put(PutRequest {
        source_filename: "/local/small.txt".into(),
        destination_filename: out_file.clone(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }
    assert!(path_to_out.exists());

    let id = user
        .put(PutRequest {
            source_filename: "/local/medium.txt".into(),
            destination_filename: new_file.clone(),
            destination_entity_id: EntityID::from(1_u16),
            transmission_mode: TransmissionMode::Acknowledged,
            filestore_requests: vec![FileStoreRequest {
                action_code: FileStoreAction::ReplaceFile,
                first_filename: out_file.clone(),
                second_filename: new_file.clone(),
            }],
            message_to_user: vec![],
        })
        .expect("unable to send put request.");
    while user
        .report(id.clone())
        .expect("Unable to obtain report.")
        .unwrap()
        .state
        != TransactionState::Terminated
    {
        thread::sleep(Duration::from_millis(5))
    }
    assert!(path_to_new.exists());

    let checksum1 = filestore
        .open(out_file, OpenOptions::new().read(true))
        .expect("Unable to open file.")
        .checksum(ChecksumType::Modular)
        .expect("Error calculating checksum.");

    let checksum2 = filestore
        .open(new_file, OpenOptions::new().read(true))
        .expect("Unable to open file.")
        .checksum(ChecksumType::Modular)
        .expect("Error calculating checksum.");

    assert_eq!(checksum1, checksum2)
}

// Series F3
// Sequence 7 Test
// Test goal:
//  - Check CreateDirectory directive
// Configuration:
//  - Acknowledged
//  - Create new directory
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s07(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/data".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::CreateDirectory,
            first_filename: out_file,
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
    assert!(path_to_out.is_dir())
}

// Series F3
// Sequence 8 Test
// Test goal:
//  - Check RemoveDirectory directive
// Configuration:
//  - Acknowledged
//  - Create new directory
//  - Then remove it
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s08(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/data_f3s8".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::CreateDirectory,
            first_filename: out_file.clone(),
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
    assert!(path_to_out.is_dir());

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::RemoveDirectory,
            first_filename: out_file,
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }

    assert!(!path_to_out.exists())
}

// Series F3
// Sequence 9 Test
// Test goal:
//  - Check DenyFile directive
// Configuration:
//  - Acknowledged
//  - Send M file
//  - Then DenyFile and verify it is removed
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s09(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "remote/medium_f3s9.txt".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "/local/medium.txt".into(),
        destination_filename: out_file.clone(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
    assert!(path_to_out.is_file());

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![FileStoreRequest {
            action_code: FileStoreAction::DenyFile,
            first_filename: out_file,
            second_filename: "".into(),
        }],
        message_to_user: vec![],
    })
    .expect("unable to send put request.");

    while path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }

    assert!(!path_to_out.exists())
}

// Series F3
// Sequence 9 Test
// Test goal:
//  - Check DirectoryListing directive
// Configuration:
//  - Acknowledged
//  - Send Directory listing request
//  - verify the listing file is created
#[rstest]
#[cfg_attr(target_os = "windows", ignore)]
#[timeout(Duration::from_secs(5))]
fn f3s10(get_filestore: &(&'static String, Arc<NativeFileStore>)) {
    let (local_path, filestore) = get_filestore;

    let mut user = User::new(Some(local_path)).expect("User Cannot connect to Daemon.");

    let out_file: Utf8PathBuf = "/local/remote.listing".into();
    let path_to_out = filestore.get_native_path(&out_file);

    user.put(PutRequest {
        source_filename: "".into(),
        destination_filename: "".into(),
        destination_entity_id: EntityID::from(1_u16),
        transmission_mode: TransmissionMode::Acknowledged,
        filestore_requests: vec![],
        message_to_user: vec![MessageToUser::from(UserOperation::Request(
            UserRequest::DirectoryListing(DirectoryListingRequest {
                directory_name: "/remote".into(),
                directory_filename: "/local/remote.listing".into(),
            }),
        ))],
    })
    .expect("unable to send put request.");

    while !path_to_out.exists() {
        thread::sleep(Duration::from_millis(1))
    }

    assert!(path_to_out.exists());
    assert!(path_to_out.is_file());
}
