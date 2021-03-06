use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

use std::io::Read;

use super::{
    error::{PDUError, PDUResult},
    header::{read_length_value_pair, PDUEncode},
};

#[repr(u8)]
#[derive(Debug, Clone, PartialEq, Eq, FromPrimitive)]
/// Actions which can be take via a FileStore Request to a CFDP entity.
pub enum FileStoreAction {
    /// Create a new file on disk.
    CreateFile = 0b0000,
    /// Delete an existing file on disk. Errors if the file does not exist.
    DeleteFile = 0b0001,
    /// Rename a file on disk. Requires a second filename.
    RenameFile = 0b0010,
    /// Append to a filename on disk. Requires a second filename
    AppendFile = 0b0011,
    /// Replace a file with one of a different name. Requites a second filename
    ReplaceFile = 0b0100,
    /// Create a new directory
    CreateDirectory = 0b0101,
    /// Delete a directory. Errors if it does not exist.
    RemoveDirectory = 0b0110,
    /// Delete a file if present. Does not fail if file does not exist.
    DenyFile = 0b0111,
    /// Remove a directory if present. Does not fail if directory does not exit.
    DenyDirectory = 0b1000,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum CreateFileStatus {
    Successful = 0b0000,
    NotAllowed = 0b0001,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum DeleteFileStatus {
    Successful = 0b0000,
    FileDoesNotExist = 0b0001,
    DeleteNotAllowed = 0b0010,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum RenameStatus {
    Successful = 0b0000,
    OldFilenameDoesNotExist = 0b0001,
    NewFilenameAlreadyExists = 0b0010,
    RenameNotAllowed = 0b0011,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum AppendStatus {
    Successful = 0b0000,
    Filename1DoesNotExist = 0b0001,
    Filename2DoesNotExist = 0b0010,
    NotAllowed = 0b0011,
    NotPerformed = 0b1111,
}
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum ReplaceStatus {
    Successful = 0b0000,
    Filename1DoesNotExist = 0b0001,
    Filename2DoesNotExist = 0b0010,
    NotAllowed = 0b0011,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum CreateDirectoryStatus {
    Successful = 0b0000,
    DirectoryCannotBeCreated = 0b0001,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum RemoveDirectoryStatus {
    Successful = 0b0000,
    DirectoryDoesNotExist = 0b0001,
    DeleteNotAllowed = 0b0110,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, FromPrimitive)]
pub enum DenyStatus {
    Successful = 0b0000,
    NotAllowed = 0b0010,
    NotPerformed = 0b1111,
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileStoreStatus {
    CreateFile(CreateFileStatus),
    DeleteFile(DeleteFileStatus),
    RenameFile(RenameStatus),
    AppendFile(AppendStatus),
    ReplaceFile(ReplaceStatus),
    CreateDirectory(CreateDirectoryStatus),
    RemoveDirectory(RemoveDirectoryStatus),
    DenyFile(DenyStatus),
    DenyDirectory(DenyStatus),
}
impl FileStoreStatus {
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::CreateFile(val) => ((FileStoreAction::CreateFile as u8) << 4) | *val as u8,
            Self::DeleteFile(val) => ((FileStoreAction::DeleteFile as u8) << 4) | *val as u8,
            Self::RenameFile(val) => ((FileStoreAction::RenameFile as u8) << 4) | *val as u8,
            Self::AppendFile(val) => ((FileStoreAction::AppendFile as u8) << 4) | *val as u8,
            Self::ReplaceFile(val) => ((FileStoreAction::ReplaceFile as u8) << 4) | *val as u8,
            Self::CreateDirectory(val) => {
                ((FileStoreAction::CreateDirectory as u8) << 4) | *val as u8
            }
            Self::RemoveDirectory(val) => {
                ((FileStoreAction::RemoveDirectory as u8) << 4) | *val as u8
            }
            Self::DenyFile(val) => ((FileStoreAction::DenyFile as u8) << 4) | *val as u8,
            Self::DenyDirectory(val) => ((FileStoreAction::DenyDirectory as u8) << 4) | *val as u8,
        }
    }

    pub fn get_status(action: &FileStoreAction, status: u8) -> PDUResult<Self> {
        match action {
            FileStoreAction::CreateFile => {
                let stat: CreateFileStatus = CreateFileStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::CreateFile),
                )?;
                Ok(Self::CreateFile(stat))
            }
            FileStoreAction::DeleteFile => {
                let stat: DeleteFileStatus = DeleteFileStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::DeleteFile),
                )?;
                Ok(Self::DeleteFile(stat))
            }
            FileStoreAction::RenameFile => {
                let stat = RenameStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::RenameFile),
                )?;
                Ok(Self::RenameFile(stat))
            }
            FileStoreAction::AppendFile => {
                let stat = AppendStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::AppendFile),
                )?;
                Ok(Self::AppendFile(stat))
            }
            FileStoreAction::ReplaceFile => {
                let stat = ReplaceStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::ReplaceFile),
                )?;
                Ok(Self::ReplaceFile(stat))
            }
            FileStoreAction::CreateDirectory => {
                let stat = CreateDirectoryStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::CreateDirectory),
                )?;
                Ok(Self::CreateDirectory(stat))
            }
            FileStoreAction::RemoveDirectory => {
                let stat = RemoveDirectoryStatus::from_u8(status).ok_or(
                    PDUError::InvalidFileStoreStatus(status, FileStoreAction::RemoveDirectory),
                )?;
                Ok(Self::RemoveDirectory(stat))
            }
            FileStoreAction::DenyFile => {
                let stat = DenyStatus::from_u8(status).ok_or(PDUError::InvalidFileStoreStatus(
                    status,
                    FileStoreAction::DenyFile,
                ))?;
                Ok(Self::DenyFile(stat))
            }
            FileStoreAction::DenyDirectory => {
                let stat = DenyStatus::from_u8(status).ok_or(PDUError::InvalidFileStoreStatus(
                    status,
                    FileStoreAction::DenyDirectory,
                ))?;
                Ok(Self::DenyDirectory(stat))
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStoreRequest {
    pub action_code: FileStoreAction,
    /// LV type field, omitted when length 0.
    pub first_filename: Vec<u8>,
    /// LV type field, omitted when length 0.
    /// Only has non-zero length for rename, append, and replace actions.
    pub second_filename: Vec<u8>,
}
impl PDUEncode for FileStoreRequest {
    type PDUType = Self;
    fn encode(self) -> Vec<u8> {
        let first_byte = (self.action_code as u8) << 4;
        let mut buffer = vec![first_byte];

        buffer.push(self.first_filename.len() as u8);
        buffer.extend(self.first_filename);

        buffer.push(self.second_filename.len() as u8);
        buffer.extend(self.second_filename);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let action_code = {
            let mut u8_buff = [0u8; 1];
            buffer.read_exact(&mut u8_buff)?;
            let possible_action = (u8_buff[0] & 0xF0) >> 4;
            FileStoreAction::from_u8(possible_action)
                .ok_or(PDUError::InvalidFileStoreAction(possible_action))?
        };
        let first_filename = read_length_value_pair(buffer)?;
        let second_filename = read_length_value_pair(buffer)?;
        Ok(Self {
            action_code,
            first_filename,
            second_filename,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStoreResponse {
    pub action_and_status: FileStoreStatus,
    /// LV type field, omitted when length 0
    pub first_filename: Vec<u8>,
    /// LV type field, omitted when length 0
    /// Only has non-zero length for rename, append, and replace actions.
    pub second_filename: Vec<u8>,
    /// LV type field, omitted when length 0
    pub filestore_message: Vec<u8>,
}
impl PDUEncode for FileStoreResponse {
    type PDUType = Self;
    fn encode(self) -> Vec<u8> {
        let mut buffer = vec![self.action_and_status.as_u8()];

        buffer.push(self.first_filename.len() as u8);
        buffer.extend(self.first_filename);

        buffer.push(self.second_filename.len() as u8);
        buffer.extend(self.second_filename);

        buffer.push(self.filestore_message.len() as u8);
        buffer.extend(self.filestore_message);

        buffer
    }

    fn decode<T: Read>(buffer: &mut T) -> PDUResult<Self::PDUType> {
        let mut u8_buff = [0u8; 1];
        buffer.read_exact(&mut u8_buff)?;
        let first_byte = u8_buff[0];

        let action_code = {
            let possible_action = (first_byte & 0xF0) >> 4;
            FileStoreAction::from_u8(possible_action)
                .ok_or(PDUError::InvalidFileStoreAction(possible_action))?
        };

        let action_and_status = FileStoreStatus::get_status(&action_code, first_byte & 0xF)?;

        let first_filename = read_length_value_pair(buffer)?;
        let second_filename = read_length_value_pair(buffer)?;
        let filestore_message = read_length_value_pair(buffer)?;
        Ok(Self {
            action_and_status,
            first_filename,
            second_filename,
            filestore_message,
        })
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use crate::assert_err;

    use rstest::rstest;

    #[rstest]
    #[case("", "/a/longer/second/name")]
    #[case("/b/longer/first/name", "")]
    #[case("/b/longer/first/name", "/a/longer/second/name")]
    #[case("", "")]
    fn filestore_request(
        #[values(
            FileStoreAction::CreateFile,
            FileStoreAction::DeleteFile,
            FileStoreAction::RenameFile,
            FileStoreAction::AppendFile,
            FileStoreAction::CreateDirectory,
            FileStoreAction::RemoveDirectory,
            FileStoreAction::DenyFile,
            FileStoreAction::DenyDirectory
        )]
        action_code: FileStoreAction,
        #[case] first_filename: &str,
        #[case] second_filename: &str,
    ) {
        let expected = FileStoreRequest {
            action_code,
            first_filename: first_filename.as_bytes().to_vec(),
            second_filename: second_filename.as_bytes().to_vec(),
        };

        let buffer = expected.clone().encode();
        let recovered = FileStoreRequest::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }

    #[rstest]
    #[case(FileStoreAction::CreateFile, 0b1110)]
    #[case(FileStoreAction::DeleteFile, 0b0100)]
    #[case(FileStoreAction::RenameFile, 0b0100)]
    #[case(FileStoreAction::AppendFile, 0b1000)]
    #[case(FileStoreAction::ReplaceFile, 0b1000)]
    #[case(FileStoreAction::CreateDirectory, 0b1000)]
    #[case(FileStoreAction::RemoveDirectory, 0b0100)]
    #[case(FileStoreAction::DenyFile, 0b0100)]
    #[case(FileStoreAction::DenyDirectory, 0b0100)]
    fn filestore_status_errors(#[case] action: FileStoreAction, #[case] status: u8) {
        assert_err!(
            FileStoreStatus::get_status(&action, status),
            Err(PDUError::InvalidFileStoreStatus(_, _))
        )
    }

    #[rstest]
    #[case("", "", "")]
    #[case("/a/longer/first/name", "", "")]
    #[case("/a/longer/first/name", "/b/longer/second/name", "")]
    #[case("/a/longer/first/name", "", "a non trivial message")]
    #[case(
        "/a/longer/first/name",
        "/b/longer/second/name",
        "a non trivial message"
    )]
    #[case("", "/b/longer/second/name", "")]
    #[case("", "/b/longer/second/name", "a non trivial message")]
    #[case("", "", "a non trivial message")]
    fn filestore_response(
        #[case] first_filename: &str,
        #[case] second_filename: &str,
        #[case] filestore_message: &str,
        #[values(
            FileStoreStatus::CreateFile(CreateFileStatus::Successful),
            FileStoreStatus::CreateFile(CreateFileStatus::NotAllowed),
            FileStoreStatus::CreateFile(CreateFileStatus::NotPerformed),
            FileStoreStatus::DeleteFile(DeleteFileStatus::Successful),
            FileStoreStatus::DeleteFile(DeleteFileStatus::FileDoesNotExist),
            FileStoreStatus::DeleteFile(DeleteFileStatus::DeleteNotAllowed),
            FileStoreStatus::DeleteFile(DeleteFileStatus::NotPerformed),
            FileStoreStatus::RenameFile(RenameStatus::Successful),
            FileStoreStatus::RenameFile(RenameStatus::OldFilenameDoesNotExist),
            FileStoreStatus::RenameFile(RenameStatus::NewFilenameAlreadyExists),
            FileStoreStatus::RenameFile(RenameStatus::RenameNotAllowed),
            FileStoreStatus::RenameFile(RenameStatus::NotPerformed),
            FileStoreStatus::AppendFile(AppendStatus::Successful),
            FileStoreStatus::AppendFile(AppendStatus::Filename1DoesNotExist),
            FileStoreStatus::AppendFile(AppendStatus::Filename2DoesNotExist),
            FileStoreStatus::AppendFile(AppendStatus::NotAllowed),
            FileStoreStatus::AppendFile(AppendStatus::NotPerformed),
            FileStoreStatus::ReplaceFile(ReplaceStatus::Successful),
            FileStoreStatus::ReplaceFile(ReplaceStatus::Filename1DoesNotExist),
            FileStoreStatus::ReplaceFile(ReplaceStatus::Filename2DoesNotExist),
            FileStoreStatus::ReplaceFile(ReplaceStatus::NotAllowed),
            FileStoreStatus::ReplaceFile(ReplaceStatus::NotPerformed),
            FileStoreStatus::CreateDirectory(CreateDirectoryStatus::Successful),
            FileStoreStatus::CreateDirectory(CreateDirectoryStatus::DirectoryCannotBeCreated),
            FileStoreStatus::CreateDirectory(CreateDirectoryStatus::NotPerformed),
            FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::Successful),
            FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::DirectoryDoesNotExist),
            FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::DeleteNotAllowed),
            FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::NotPerformed),
            FileStoreStatus::DenyFile(DenyStatus::Successful),
            FileStoreStatus::DenyFile(DenyStatus::NotAllowed),
            FileStoreStatus::DenyFile(DenyStatus::NotPerformed),
            FileStoreStatus::DenyDirectory(DenyStatus::Successful),
            FileStoreStatus::DenyDirectory(DenyStatus::NotAllowed),
            FileStoreStatus::DenyDirectory(DenyStatus::NotPerformed)
        )]
        action_and_status: FileStoreStatus,
    ) {
        let expected = FileStoreResponse {
            action_and_status,
            first_filename: first_filename.as_bytes().to_vec(),
            second_filename: second_filename.as_bytes().to_vec(),
            filestore_message: filestore_message.as_bytes().to_vec(),
        };

        let buffer = expected.clone().encode();
        let recovered = FileStoreResponse::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }
}
