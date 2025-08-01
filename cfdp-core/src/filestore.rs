use std::{
    fmt::Write as _Write,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Error as IOError, ErrorKind, Read, Seek, Write},
    str::Utf8Error,
    time::{SystemTime, SystemTimeError},
};

use camino::{Utf8Component, Utf8Path, Utf8PathBuf};
use num_derive::FromPrimitive;
use pathdiff::diff_paths;
use tempfile::tempfile;
use thiserror::Error;

pub use crate::pdu::{
    AppendStatus, CreateDirectoryStatus, CreateFileStatus, DeleteFileStatus, DenyStatus,
    FileStoreAction, FileStoreRequest, FileStoreResponse, FileStoreStatus, RemoveDirectoryStatus,
    RenameStatus, ReplaceStatus,
};

// file path normalization taken from cargo
// https://github.com/rust-lang/cargo/blob/6d6dd9d9be9c91390da620adf43581619c2fa90e/crates/cargo-util/src/paths.rs#L81
// This has been modified as follows:
//   -  Does accept root dir `/` as the first entry.
//   - Operators on Utf8Paths from the camino crate
fn normalize_path(path: &Utf8Path) -> Utf8PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Utf8Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        Utf8PathBuf::from(c.as_str())
    } else {
        Utf8PathBuf::new()
    };
    // if the path begins with any number of root dir components skip them
    while let Some(_c @ Utf8Component::RootDir) = components.peek().cloned() {
        components.next();
    }

    for component in components {
        match component {
            Utf8Component::Prefix(..) => unreachable!(),
            Utf8Component::RootDir => {
                unreachable!()
            }
            Utf8Component::CurDir => {}
            Utf8Component::ParentDir => {
                ret.pop();
            }
            Utf8Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

pub type FileStoreResult<T> = Result<T, FileStoreError>;
#[derive(Error, Debug)]
pub enum FileStoreError {
    #[error("File data storage error: {0}")]
    IO(#[from] IOError),
    #[error("Error Formatting String: {0}")]
    Format(#[from] std::fmt::Error),
    #[error("Error getting SystemTime: {0}")]
    SystemTime(#[from] SystemTimeError),
    #[error("Cannot find relative path between {0:} and {1:}.")]
    PathDiff(String, String),
    #[error("Error converting string from UTF-8: {0:}")]
    UTF8(#[from] Utf8Error),
}

/// Defines any necessary actions a CFDP File Store implementation
/// must perform. Assumes any FileStore has a root path it operates relative to.
pub trait FileStore {
    /// Returns the path to the target with the root path prepended.
    /// Used when manipulating the filesystem relative to the root path.
    fn get_native_path<P: AsRef<Utf8Path>>(&self, path: P) -> Utf8PathBuf;

    /// Creates a file relative to the root path.
    fn create_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Delete a file relative to the root path.
    fn delete_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Renames a file relative to the root path.
    /// Renames 'from' to 'to'.
    fn rename_file<P: AsRef<Utf8Path>, U: AsRef<Utf8Path>>(
        &self,
        from: P,
        to: U,
    ) -> FileStoreResult<()>;

    /// Appends the contents of File 2 into File 1.
    /// Both paths are assumed to be relative to the root path.
    fn append_file<P: AsRef<Utf8Path>, U: AsRef<Utf8Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()>;

    /// Replace the contents of File 1 with the contents of File 2.
    /// Both paths are assumed relative to the root path.
    fn replace_file<P: AsRef<Utf8Path>, U: AsRef<Utf8Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()>;

    /// Creates the directory Relative to the root path.
    fn create_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Remove a directory Relative to the root path.
    fn remove_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()>;

    /// List the Contents of a directory relative to the root path.
    fn list_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<String>;

    /// Opens a file relative to the root path with the given options.
    fn open<P: AsRef<Utf8Path>>(&self, path: P, options: &mut OpenOptions)
        -> FileStoreResult<File>;

    /// Opens a system temporary file
    fn open_tempfile(&self) -> FileStoreResult<File>;

    /// Returns the size of the file on disk relative to the root path.
    fn get_size<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<u64>;

    /// Executes an action based on an input filestore request
    /// This is meant to be executed by a [Send](crate::transaction::SendTransaction) or [Recv](crate::transaction::RecvTransaction) transaction.
    /// instance which requires errors be mapped to a status.
    fn process_request(&self, request: &FileStoreRequest) -> FileStoreResponse {
        let path = self.get_native_path(&request.first_filename);
        let path2 = self.get_native_path(&request.second_filename);

        let action_and_status = match request.action_code {
            FileStoreAction::CreateFile => match path.exists() {
                false => match self.create_file(path) {
                    Ok(()) => FileStoreStatus::CreateFile(CreateFileStatus::Successful),
                    Err(_) => FileStoreStatus::CreateFile(CreateFileStatus::NotAllowed),
                },
                true => FileStoreStatus::CreateFile(CreateFileStatus::NotAllowed),
            },
            FileStoreAction::DeleteFile => match path.is_file() {
                true => match self.delete_file(path) {
                    Ok(()) => FileStoreStatus::DeleteFile(DeleteFileStatus::Successful),

                    Err(_) => FileStoreStatus::DeleteFile(DeleteFileStatus::DeleteNotAllowed),
                },
                false => FileStoreStatus::DeleteFile(DeleteFileStatus::FileDoesNotExist),
            },
            FileStoreAction::RenameFile => match path.is_file() {
                true => match path2.is_file() {
                    false => match self.rename_file(path, path2) {
                        Ok(()) => FileStoreStatus::RenameFile(RenameStatus::Successful),
                        Err(_) => FileStoreStatus::RenameFile(RenameStatus::RenameNotAllowed),
                    },
                    true => FileStoreStatus::RenameFile(RenameStatus::NewFilenameAlreadyExists),
                },
                false => FileStoreStatus::RenameFile(RenameStatus::OldFilenameDoesNotExist),
            },
            FileStoreAction::AppendFile => match path.is_file() {
                true => match path2.is_file() {
                    true => match self.append_file(&path, &path2) {
                        Ok(()) => FileStoreStatus::AppendFile(AppendStatus::Successful),
                        Err(_) => FileStoreStatus::AppendFile(AppendStatus::NotAllowed),
                    },
                    false => FileStoreStatus::AppendFile(AppendStatus::Filename2DoesNotExist),
                },
                false => FileStoreStatus::AppendFile(AppendStatus::Filename1DoesNotExist),
            },
            FileStoreAction::ReplaceFile => match path.is_file() {
                true => match path2.is_file() {
                    true => match self.replace_file(&path, &path2) {
                        Ok(()) => FileStoreStatus::ReplaceFile(ReplaceStatus::Successful),
                        Err(_) => FileStoreStatus::ReplaceFile(ReplaceStatus::NotAllowed),
                    },
                    false => FileStoreStatus::ReplaceFile(ReplaceStatus::Filename2DoesNotExist),
                },
                false => FileStoreStatus::ReplaceFile(ReplaceStatus::Filename1DoesNotExist),
            },
            FileStoreAction::CreateDirectory => match path.is_dir() {
                false => match self.create_directory(path) {
                    Ok(()) => FileStoreStatus::CreateDirectory(CreateDirectoryStatus::Successful),
                    Err(_) => FileStoreStatus::CreateDirectory(
                        CreateDirectoryStatus::DirectoryCannotBeCreated,
                    ),
                },
                true => FileStoreStatus::CreateDirectory(
                    CreateDirectoryStatus::DirectoryCannotBeCreated,
                ),
            },
            FileStoreAction::RemoveDirectory => match path.is_dir() {
                true => match self.remove_directory(path) {
                    Ok(()) => FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::Successful),
                    Err(_) => {
                        FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::DeleteNotAllowed)
                    }
                },
                false => {
                    FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::DirectoryDoesNotExist)
                }
            },
            // Deny ignores all errors
            FileStoreAction::DenyFile => match path.is_file() {
                true => match self.delete_file(path) {
                    Ok(()) => FileStoreStatus::DenyFile(DenyStatus::Successful),
                    Err(_) => FileStoreStatus::DenyFile(DenyStatus::NotAllowed),
                },
                false => FileStoreStatus::DenyFile(DenyStatus::NotAllowed),
            },
            // Deny ignores all errors
            FileStoreAction::DenyDirectory => match path.is_dir() {
                true => match self.remove_directory(path) {
                    Ok(()) => FileStoreStatus::DenyDirectory(DenyStatus::Successful),
                    Err(_) => FileStoreStatus::DenyDirectory(DenyStatus::NotAllowed),
                },
                false => FileStoreStatus::DenyDirectory(DenyStatus::NotAllowed),
            },
        };
        FileStoreResponse {
            action_and_status,
            first_filename: request.first_filename.clone(),
            second_filename: request.second_filename.clone(),
            filestore_message: vec![],
        }
    }
}

/// Store the root path information for a FileStore implementation
/// using built in rust [std::fs] interface.
pub struct NativeFileStore {
    root_path: Utf8PathBuf,
}
impl NativeFileStore {
    pub fn new<P: AsRef<Utf8Path>>(root_path: P) -> Self {
        Self {
            root_path: root_path.as_ref().to_owned(),
        }
    }
}
impl FileStore for NativeFileStore {
    fn get_native_path<P: AsRef<Utf8Path>>(&self, path: P) -> Utf8PathBuf {
        let path = path.as_ref();
        match path.starts_with(&self.root_path) {
            true => path.to_path_buf(),
            false => {
                let normal_path = normalize_path(path);
                self.root_path.join(normal_path)
            }
        }
    }

    /// This is a wrapper around [File::create]
    fn create_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let path = self.get_native_path(path);
        let f = File::create(path)?;
        f.sync_all().map_err(FileStoreError::IO)
    }

    /// This is a wrapper around [fs::remove_file]
    fn delete_file<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::remove_file(full_path)?;
        Ok(())
    }

    /// This is a wrapper around [fs::rename]
    fn rename_file<P: AsRef<Utf8Path>, U: AsRef<Utf8Path>>(
        &self,
        from: P,
        to: U,
    ) -> FileStoreResult<()> {
        let full_from_path = self.get_native_path(from);
        let full_to_path = self.get_native_path(to);
        // CFDP expects to not be allowed if the "TO" file already exists
        // so add an extra check since fs::rename doesn't care
        match full_to_path.exists() {
            true => Err(IOError::from(ErrorKind::AlreadyExists).into()),
            false => fs::rename(full_from_path, full_to_path).map_err(FileStoreError::IO),
        }
    }

    /// This function uses [fs::read] to append the contents of path2 to path1.
    fn append_file<P: AsRef<Utf8Path>, U: AsRef<Utf8Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()> {
        let full_path1 = self.get_native_path(path1);
        let full_path2 = self.get_native_path(path2);

        {
            let mut open_file1 = File::options().append(true).open(full_path1)?;
            open_file1.write_all(fs::read(full_path2)?.as_slice())?;
        }

        Ok(())
    }

    fn replace_file<P: AsRef<Utf8Path>, U: AsRef<Utf8Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()> {
        let full_path1 = self.get_native_path(path1);
        let full_path2 = self.get_native_path(path2);
        match full_path1.exists() {
            true => fs::write(full_path1, fs::read(full_path2)?).map_err(FileStoreError::IO),
            false => Err(IOError::from(ErrorKind::NotFound).into()),
        }
    }

    /// This is a wrapper around [fs::create_dir]
    fn create_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::create_dir(full_path)?;
        Ok(())
    }

    /// This function wraps [fs::remove_dir_all]
    fn remove_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::remove_dir_all(full_path)?;
        Ok(())
    }

    fn list_directory<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<String> {
        let directory = self.get_native_path(path);
        let mut directory_listing =
            format!("Listing for directory: {directory}\ntype,path,size,timestamp\n",);
        let (mut dirs, mut files): (Vec<_>, Vec<_>) = fs::read_dir(&directory)?
            .filter_map(|entry| entry.ok())
            .partition(|entry| entry.path().is_dir());

        dirs.sort_by_key(|dir| dir.path());
        files.sort_by_key(|file| file.path());

        for dir in dirs {
            let meta = dir.metadata()?;
            writeln!(
                directory_listing,
                "d,{},{},{}",
                diff_paths(dir.path(), &directory)
                    .ok_or_else(|| FileStoreError::PathDiff(
                        dir.path().display().to_string(),
                        directory.to_string(),
                    ))?
                    .display(),
                meta.len(),
                meta.modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs()
            )?;
        }

        for file in files {
            let meta = file.metadata()?;
            writeln!(
                directory_listing,
                "f,{},{},{}",
                diff_paths(file.path(), &directory)
                    .ok_or_else(|| FileStoreError::PathDiff(
                        file.path().display().to_string(),
                        directory.to_string(),
                    ))?
                    .display(),
                meta.len(),
                meta.modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs()
            )?;
        }

        Ok(directory_listing)
    }

    fn open<P: AsRef<Utf8Path>>(
        &self,
        path: P,
        options: &mut OpenOptions,
    ) -> FileStoreResult<File> {
        let full_path = self.get_native_path(path);
        Ok(options.open(full_path)?)
    }

    /// This is an alias for [tempfile::tempfile]
    fn open_tempfile(&self) -> FileStoreResult<File> {
        Ok(tempfile()?)
    }

    /// This function uses [fs::metadata] to read the size of the input file relative to the root path.
    fn get_size<P: AsRef<Utf8Path>>(&self, path: P) -> FileStoreResult<u64> {
        let full_path = self.get_native_path(path);
        Ok(fs::metadata(full_path)?.len())
    }
}

#[repr(u8)]
#[derive(Debug, Clone, Copy, FromPrimitive, PartialEq, Eq)]
/// CCSDS enumerated checksum types
pub enum ChecksumType {
    /// Turn every 4 bytes into a u32 and accumulate
    Modular = 0,
    /// This checksum is always 0
    Null = 15,
}

/// Computes all pre-defined CCSDS checksums
pub trait FileChecksum {
    /// Given the input [ChecksumType] compute the appropriate algorithm.
    fn checksum(&mut self, checksum_type: ChecksumType) -> FileStoreResult<u32>;
}

impl<R: Read + Seek + ?Sized> FileChecksum for R {
    fn checksum(&mut self, checksum_type: ChecksumType) -> FileStoreResult<u32> {
        match checksum_type {
            ChecksumType::Null => Ok(0_u32),
            ChecksumType::Modular => {
                let mut reader = BufReader::new(self);
                // reset the file pointer to the beginning
                reader.rewind()?;

                let mut checksum: u32 = 0;
                'outer: loop {
                    // fill_buffer will return an empty slice when EoF is reached
                    // on the internal Read instance
                    let buffer = reader.fill_buf()?;

                    if buffer.is_empty() {
                        // if nothing was read break from the loop

                        break 'outer;
                    }

                    // chunks_exact can some times be more efficient than chunks
                    // we'll have to deal with the remainder anyway.
                    // Take 4 bytes at a time from the buffer, convert to u32 and add
                    let mut iter = buffer.chunks_exact(4);
                    (&mut iter).for_each(|chunk| {
                        // we can unwrap because we are guaranteed to have a length 4 slice
                        checksum =
                            checksum.wrapping_add(u32::from_be_bytes(chunk.try_into().unwrap()));
                    });
                    // handle any remainder by resizing to 4-bytes then adding
                    if !iter.remainder().is_empty() {
                        let mut remainder = iter.remainder().to_vec();
                        remainder.resize(4, 0_u8);
                        // we can unwrap because we are guaranteed to have a length 4 vector
                        checksum = checksum
                            .wrapping_add(u32::from_be_bytes(remainder.try_into().unwrap()));
                    }

                    let len = buffer.len();
                    // update the internal buffer to let it know
                    // len bytes were consumed
                    reader.consume(len);
                }
                Ok(checksum)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::collections::HashMap;

    use rstest::*;
    use tempfile::TempDir;

    #[fixture]
    #[once]
    fn tempdir_fixture() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    #[once]
    fn test_filestore(tempdir_fixture: &TempDir) -> NativeFileStore {
        NativeFileStore::new(
            Utf8Path::from_path(tempdir_fixture.path()).expect("Unable to make utf8 tempdir"),
        )
    }

    #[rstest]
    fn create_file(test_filestore: &NativeFileStore) {
        let path = Utf8Path::new("create_file.txt");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);

        assert!(full_path.exists())
    }

    #[rstest]
    fn delete_file(test_filestore: &NativeFileStore) {
        let path = Utf8Path::new("delete_file.txt");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);
        assert!(full_path.exists());

        test_filestore.delete_file(path).unwrap();

        assert!(!full_path.exists())
    }

    #[rstest]
    fn rename_file(test_filestore: &NativeFileStore) {
        let path = Utf8Path::new("rename_init.txt");
        let new_path = Utf8Path::new("rename_new.dat");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);
        assert!(full_path.exists());

        test_filestore.rename_file(path, new_path).unwrap();

        assert!(!path.exists());
        assert!(!new_path.exists())
    }

    #[rstest]
    fn append_file(test_filestore: &NativeFileStore) {
        let path = "append+_path1.txt";

        let mut options = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .to_owned();
        {
            let mut file = test_filestore.open(path, &mut options).unwrap();
            file.write_all("test text".as_bytes()).unwrap();
        }

        let path2 = "append_path2.txt";
        {
            let mut file = test_filestore.open(path2, &mut options).unwrap();
            file.write_all("new text".as_bytes()).unwrap();
        }

        test_filestore.append_file(path, path2).unwrap();

        let expected = "test textnew text".to_owned(); // cspell:disable-line

        let mut recovered_text = String::with_capacity(expected.len());
        test_filestore
            .open(path, OpenOptions::new().read(true))
            .unwrap()
            .read_to_string(&mut recovered_text)
            .unwrap();
        println!("{:?}", test_filestore.get_native_path(path));
        assert_eq!(expected, recovered_text)
    }

    #[rstest]
    fn replace_file(test_filestore: &NativeFileStore) {
        let mut options = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .to_owned();
        let path = "replace_path1.txt";
        {
            let mut file = test_filestore.open(path, &mut options).unwrap();
            file.write_all("test text".as_bytes()).unwrap();
        }

        let path2 = "replace_path2.txt";
        {
            let mut file = test_filestore.open(path2, &mut options).unwrap();
            file.write_all("new text".as_bytes()).unwrap();
        }

        test_filestore.replace_file(path, path2).unwrap();

        let expected = "new text".to_owned();
        let mut recovered_text = String::new();
        test_filestore
            .open(path, OpenOptions::new().read(true))
            .unwrap()
            .read_to_string(&mut recovered_text)
            .unwrap();

        assert_eq!(expected, recovered_text)
    }

    #[rstest]
    fn create_remove_dir(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        let dir_path = Utf8Path::new("/help");
        let full_path = test_filestore.get_native_path(dir_path);
        test_filestore.create_directory(dir_path)?;

        assert!(full_path.is_dir());
        assert!(full_path.exists());

        test_filestore.remove_directory(dir_path)?;

        assert!(!full_path.exists());
        Ok(())
    }

    #[rstest]
    fn create_tmpfile(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        let mut file = test_filestore.open_tempfile()?;

        {
            file.write_all("hello, world!".as_bytes())?;
            file.sync_all()?;
        }
        file.rewind()?;
        let mut recovered_text = String::new();
        file.read_to_string(&mut recovered_text)?;

        assert_eq!("hello, world!".to_owned(), recovered_text);
        Ok(())
    }

    #[rstest]
    fn get_filesize(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        let input_text = "Hello, world!";
        let expected = input_text.len() as u64;
        {
            let mut file =
                test_filestore.open("test.dat", OpenOptions::new().create(true).write(true))?;
            file.write_all(input_text.as_bytes())?;
            file.sync_all()?;
        }

        let size = test_filestore.get_size("test.dat")?;

        assert_eq!(expected, size);
        Ok(())
    }

    #[rstest]
    fn list_dir(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        test_filestore.create_directory("listing")?;
        let basepath = Utf8Path::new("listing");

        let input_text = "Hello, world!";
        let input_text2 = "A longer string of text for this file.\n";

        let mut dir_size = HashMap::<&str, u64>::new();

        let mut timings = HashMap::<&str, u64>::new();

        for dirname in ["one", "two", "three"] {
            let dname = basepath.join(dirname);
            test_filestore.create_directory(&dname)?;
            let meta = fs::metadata(test_filestore.get_native_path(dname))
                .expect("No directory for metadata.");
            timings.insert(
                dirname,
                meta.modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
            );
            dir_size.insert(dirname, meta.len());
        }

        for (filename, text) in ["test.txt", "new.dat"]
            .iter()
            .zip([input_text, input_text2].iter())
        {
            let fname = basepath.join(filename);
            {
                let mut file =
                    test_filestore.open(&fname, OpenOptions::new().create(true).write(true))?;
                file.write_all(text.as_bytes())?;
                file.sync_all()?;
            }
            timings.insert(
                filename,
                fs::metadata(test_filestore.get_native_path(fname))
                    .expect("No file for metadata.")
                    .modified()?
                    .duration_since(SystemTime::UNIX_EPOCH)?
                    .as_secs(),
            );
        }

        let expected_listing = format!(
            "Listing for directory: {dir}\ntype,path,size,timestamp
d,one,{s1},{t1}
d,three,{s2},{t2}
d,two,{s3},{t3}
f,new.dat,{s4},{t4}
f,test.txt,{s5},{t5}
",
            dir = test_filestore.get_native_path(basepath),
            t1 = timings.get("one").unwrap(),
            t2 = timings.get("three").unwrap(),
            t3 = timings.get("two").unwrap(),
            t4 = timings.get("new.dat").unwrap(),
            t5 = timings.get("test.txt").unwrap(),
            s1 = dir_size.get("one").unwrap(),
            s2 = dir_size.get("three").unwrap(),
            s3 = dir_size.get("two").unwrap(),
            s4 = input_text2.len(),
            s5 = input_text.len(),
        );
        let listing = test_filestore.list_directory("listing")?;
        assert_eq!(expected_listing, listing);
        Ok(())
    }

    #[rstest]
    fn checksum_cursor(
        #[values(ChecksumType::Null, ChecksumType::Modular)] checksum_type: ChecksumType,
    ) -> FileStoreResult<()> {
        let file_data: Vec<u8> = vec![0x8a, 0x1b, 0x37, 0x44, 0x78, 0x91, 0xab, 0x03, 0x46, 0x12];

        let expected_checksum = match &checksum_type {
            ChecksumType::Null => 0_u32,
            ChecksumType::Modular => 0x48BEE247_u32,
        };

        let recovered_checksum = std::io::Cursor::new(file_data).checksum(checksum_type)?;

        assert_eq!(expected_checksum, recovered_checksum);
        Ok(())
    }

    #[rstest]
    fn checksum_file(
        test_filestore: &NativeFileStore,
        #[values(ChecksumType::Null, ChecksumType::Modular)] checksum_type: ChecksumType,
    ) -> FileStoreResult<()> {
        let file_data: Vec<u8> = vec![0x8a, 0x1b, 0x37, 0x44, 0x78, 0x91, 0xab, 0x03, 0x46, 0x12];

        {
            let mut file = test_filestore.open(
                "checksum.txt",
                OpenOptions::new().create(true).truncate(true).write(true),
            )?;
            file.write_all(file_data.as_slice())?;
            file.sync_all()?;
        }
        let expected_checksum = match &checksum_type {
            ChecksumType::Null => 0_u32,
            ChecksumType::Modular => 0x48BEE247_u32,
        };

        let recovered_checksum = {
            let mut file =
                test_filestore.open("checksum.txt", OpenOptions::new().create(false).read(true))?;
            file.checksum(checksum_type)?
        };

        assert_eq!(expected_checksum, recovered_checksum);
        Ok(())
    }

    // We've already tested the functionality of all the actions.
    // just testing that passthrough works okay.
    #[rstest]
    fn process_request(
        #[values(
            FileStoreAction::CreateFile,
            FileStoreAction::DeleteFile,
            FileStoreAction::RenameFile,
            FileStoreAction::AppendFile,
            FileStoreAction::ReplaceFile,
            FileStoreAction::CreateDirectory,
            FileStoreAction::RemoveDirectory,
            FileStoreAction::DenyFile,
            FileStoreAction::DenyDirectory
        )]
        action_code: FileStoreAction,
    ) -> FileStoreResult<()> {
        let dir = TempDir::new().unwrap();
        let filestore = NativeFileStore::new(
            Utf8Path::from_path(dir.path()).expect("Unable to make utf8 tempdir"),
        );

        let path = "/the_first_filename";
        let path2 = "/the_second_filename";

        match &action_code {
            FileStoreAction::DeleteFile | FileStoreAction::DenyFile => {
                filestore.create_file(path).expect("Unable to create.")
            }
            FileStoreAction::RemoveDirectory | FileStoreAction::DenyDirectory => {
                filestore.create_directory(path)?
            }
            FileStoreAction::RenameFile => {
                let filename1 = filestore.get_native_path(path);
                fs::write(filename1, "test input\ntext\n")?;
            }
            FileStoreAction::AppendFile | FileStoreAction::ReplaceFile => {
                let filename1 = filestore.get_native_path(path);
                fs::write(filename1, "test input\ntext\n")?;

                let filename2 = filestore.get_native_path(path2);
                fs::write(filename2, "more\ntext\nhere!")?;
            }
            FileStoreAction::CreateFile | FileStoreAction::CreateDirectory => {}
        };

        let request = FileStoreRequest {
            action_code,
            first_filename: path.into(),
            second_filename: path2.into(),
        };
        let response = filestore.process_request(&request);
        assert!(!response.action_and_status.is_fail());
        dir.close()?;
        Ok(())
    }

    #[fixture]
    #[once]
    fn failure_dir() -> TempDir {
        TempDir::new().unwrap()
    }

    #[fixture]
    #[once]
    fn failure_filestore(failure_dir: &TempDir) -> NativeFileStore {
        let filestore = NativeFileStore::new(
            Utf8Path::from_path(failure_dir.path()).expect("Unable to make utf8 tempdir"),
        );
        filestore.create_file("file1").unwrap();
        filestore.create_file("file2").unwrap();

        filestore.create_directory("dir1").unwrap();
        filestore.create_directory("dir2").unwrap();

        filestore
    }

    #[rstest]
    #[case(
        FileStoreRequest { action_code: FileStoreAction::CreateFile, first_filename: "dir1".into(), second_filename: "".into() },
        FileStoreStatus::CreateFile(CreateFileStatus::NotAllowed)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::DeleteFile, first_filename: "test".into(), second_filename:"".into()  },
        FileStoreStatus::DeleteFile(DeleteFileStatus::FileDoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::DeleteFile, first_filename: "dir1".into(), second_filename: "".into()  },
        FileStoreStatus::DeleteFile(DeleteFileStatus::FileDoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::RenameFile, first_filename: "a".into(), second_filename: "new_file".into() },
        FileStoreStatus::RenameFile(RenameStatus::OldFilenameDoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::RenameFile, first_filename: "file1".into(), second_filename: "file2".into() },
        FileStoreStatus::RenameFile(RenameStatus::NewFilenameAlreadyExists)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::AppendFile, first_filename: "a".into(), second_filename: "file2".into() },
        FileStoreStatus::AppendFile(AppendStatus::Filename1DoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::AppendFile, first_filename: "file1".into(), second_filename: "not_real".into() },
        FileStoreStatus::AppendFile(AppendStatus::Filename2DoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::ReplaceFile, first_filename: "a".into(), second_filename: "file2".into() },
        FileStoreStatus::ReplaceFile(ReplaceStatus::Filename1DoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::ReplaceFile, first_filename: "file1".into(), second_filename: "not_real".into() },
        FileStoreStatus::ReplaceFile(ReplaceStatus::Filename2DoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::CreateDirectory, first_filename: ".".into(), second_filename: "".into()},
        FileStoreStatus::CreateDirectory(CreateDirectoryStatus::DirectoryCannotBeCreated)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::RemoveDirectory, first_filename: "not_a_dir".into(), second_filename: "".into()},
        FileStoreStatus::RemoveDirectory(RemoveDirectoryStatus::DirectoryDoesNotExist)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::DenyDirectory, first_filename: "not_a_dir".into(), second_filename: "".into()},
        FileStoreStatus::DenyDirectory(DenyStatus::NotAllowed)
    )]
    #[case(
        FileStoreRequest{action_code: FileStoreAction::DenyFile, first_filename: "not_a.file".into(), second_filename: "".into()},
        FileStoreStatus::DenyFile(DenyStatus::NotAllowed)
    )]
    fn process_failures(
        #[case] request: FileStoreRequest,
        #[case] expected: FileStoreStatus,
        failure_filestore: &NativeFileStore,
    ) {
        let response = failure_filestore.process_request(&request);
        assert_eq!(expected, response.action_and_status)
    }
}
