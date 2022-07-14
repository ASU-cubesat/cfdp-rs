use std::{
    error::Error,
    fmt::{self, Display, Write as _Write},
    fs::{self, File, OpenOptions},
    io::{Error as IOError, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    time::{SystemTime, SystemTimeError},
};

use pathdiff::diff_paths;
use tempfile::tempfile;

// file path normalization taken from cargo
// https://github.com/rust-lang/cargo/blob/6d6dd9d9be9c91390da620adf43581619c2fa90e/crates/cargo-util/src/paths.rs#L81
// This has been edited to not accept root dir `/` as the first entry.
fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components().peekable();
    let mut ret = if let Some(c @ Component::Prefix(..)) = components.peek().cloned() {
        components.next();
        PathBuf::from(c.as_os_str())
    } else {
        PathBuf::new()
    };
    // if the path begins with any number of rootdir components skip them
    while let Some(_c @ Component::RootDir) = components.peek().cloned() {
        components.next();
    }

    for component in components {
        match component {
            Component::Prefix(..) => unreachable!(),
            Component::RootDir => {
                ret.push(component.as_os_str());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                ret.pop();
            }
            Component::Normal(c) => {
                ret.push(c);
            }
        }
    }
    ret
}

pub type FileStoreResult<T> = Result<T, FileStoreError>;
#[derive(Debug)]
pub enum FileStoreError {
    IO(IOError),
    Format(std::fmt::Error),
    SystemTime(SystemTimeError),
    PathDiff(String, String),
}
impl Display for FileStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IO(source) => source.fmt(f),
            Self::Format(source) => source.fmt(f),
            Self::SystemTime(source) => source.fmt(f),
            Self::PathDiff(source1, source2) => write!(
                f,
                "Cannot find relative path between {} and {}.",
                source1, source2
            ),
        }
    }
}
impl Error for FileStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::IO(source) => Some(source),
            Self::Format(source) => Some(source),
            Self::SystemTime(source) => Some(source),
            Self::PathDiff(_, _) => None,
        }
    }
}

impl From<std::io::Error> for FileStoreError {
    fn from(err: std::io::Error) -> Self {
        Self::IO(err)
    }
}

impl From<std::fmt::Error> for FileStoreError {
    fn from(err: std::fmt::Error) -> Self {
        Self::Format(err)
    }
}
impl From<SystemTimeError> for FileStoreError {
    fn from(err: SystemTimeError) -> Self {
        Self::SystemTime(err)
    }
}

/// Defines any necessary actions a CFDP File Store implementation
/// must perform. Assumes any FileStore has a root path it operates retalive to.
pub trait FileStore {
    /// Returns the path to the target with the root path prepended.
    /// Used when manipulating the filesystem relative to the root path.
    fn get_native_path<P: AsRef<Path>>(&self, path: P) -> PathBuf;

    /// Creates a file relative to the root path.
    fn create_file<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Delete a file retlative to the root path.
    fn delete_file<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Renames a file relative to the root path.
    /// Renames 'from' to 'to'.
    fn rename_file<P: AsRef<Path>, U: AsRef<Path>>(&self, from: P, to: U) -> FileStoreResult<()>;

    /// Appends the contents of File 2 into File 1.
    /// Both paths are assumed to be relative to the root path.
    fn append_file<P: AsRef<Path>, U: AsRef<Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()>;

    /// Replace the contents of File 1 with the contents of File 2.
    /// Both paths are assumed relative to the root path.
    fn replace_file<P: AsRef<Path>, U: AsRef<Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()>;

    /// Creates the directory Relative to the root path.
    fn create_directory<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()>;

    /// Remove a directory Relative to the root path.
    fn remove_directory<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()>;

    /// List the Contents of a directory relative to the root path.
    fn list_directory<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<String>;

    /// Opens a file relative to the root path with the given options.
    fn open<P: AsRef<Path>>(&self, path: P, options: &mut OpenOptions) -> FileStoreResult<File>;

    /// Opens a system temporary file
    fn open_tempfile(&self) -> FileStoreResult<File>;

    /// Retuns the size of the file on disk relative to the root path.
    fn get_size<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<u64>;
}

/// Store the root path infomration for a FileStore implementation
/// using built in rust [std::fs] interface.
pub struct NativeFileStore {
    root_path: PathBuf,
}
impl NativeFileStore {
    pub fn new<P: AsRef<Path>>(root_path: P) -> Self {
        Self {
            root_path: root_path.as_ref().to_path_buf(),
        }
    }
}
impl FileStore for NativeFileStore {
    fn get_native_path<P: AsRef<Path>>(&self, path: P) -> PathBuf {
        let path = normalize_path(path.as_ref());
        self.root_path.join(path)
    }

    /// This is a wrapper around [File::create]
    fn create_file<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()> {
        {
            File::create(self.get_native_path(path))?;
        }
        Ok(())
    }

    /// This is a wrapper around [fs::remove_file]
    fn delete_file<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::remove_file(full_path)?;
        Ok(())
    }

    /// This is a wrapper around [fs::rename]
    fn rename_file<P: AsRef<Path>, U: AsRef<Path>>(&self, from: P, to: U) -> FileStoreResult<()> {
        let full_from_path = self.get_native_path(from);
        let full_to_path = self.get_native_path(to);
        fs::rename(full_from_path, full_to_path)?;
        Ok(())
    }

    /// This funtion uses [fs::read] to append the contents of path2 to path1.
    fn append_file<P: AsRef<Path>, U: AsRef<Path>>(
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

    fn replace_file<P: AsRef<Path>, U: AsRef<Path>>(
        &self,
        path1: P,
        path2: U,
    ) -> FileStoreResult<()> {
        let full_path1 = self.get_native_path(path1);
        let full_path2 = self.get_native_path(path2);

        fs::write(full_path1, fs::read(full_path2)?)?;
        Ok(())
    }

    /// This is a wrapper around [fs::create_dir]
    fn create_directory<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::create_dir(full_path)?;
        Ok(())
    }

    /// This function wraps [fs::remove_dir_all]
    fn remove_directory<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<()> {
        let full_path = self.get_native_path(path);
        fs::remove_dir_all(full_path)?;
        Ok(())
    }

    fn list_directory<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<String> {
        let directory = self.get_native_path(path);
        let mut directory_listing = format!(
            "Listing for directory: {}\ntype,path,size,timestamp\n",
            directory.display(),
        );
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
                        directory.display().to_string(),
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
                        directory.display().to_string(),
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

    fn open<P: AsRef<Path>>(&self, path: P, options: &mut OpenOptions) -> FileStoreResult<File> {
        let full_path = self.get_native_path(path);
        Ok(options.open(full_path)?)
    }

    /// This is an alias for [tempfile::tempfile]
    fn open_tempfile(&self) -> FileStoreResult<File> {
        Ok(tempfile()?)
    }

    /// This function uses [fs::metadata] to read the size of the input file relative to the root path.
    fn get_size<P: AsRef<Path>>(&self, path: P) -> FileStoreResult<u64> {
        let full_path = self.get_native_path(path);
        Ok(fs::metadata(full_path)?.len())
    }
}

#[repr(u8)]
#[derive(Debug, Clone)]
pub enum ChecksumType {
    Modular = 0,
    Null = 15,
}
pub trait FileChecksum {
    fn checksum(&mut self, checksum_type: ChecksumType) -> FileStoreResult<u32>;
}

impl FileChecksum for File {
    fn checksum(&mut self, checksum_type: ChecksumType) -> FileStoreResult<u32> {
        match checksum_type {
            ChecksumType::Null => Ok(0_u32),
            ChecksumType::Modular => {
                let mut checksum = 0_u32;
                // reset the file pointer to the beginning
                self.seek(SeekFrom::Start(0))?;

                loop {
                    let mut u32_buff = Vec::with_capacity(4);
                    // this line will take 4 bytes at a time from the file.
                    // if there are less that 4 bytes left it will right pad the buffer with 0s.
                    let num_read = Read::take(Read::by_ref(self), 4).read_to_end(&mut u32_buff)?;
                    match num_read {
                        // if nothing was read break from the loop
                        0 => break,
                        // otherwise add the 4 bytes as a u32 to the checksum
                        num => {
                            // If the file size is not a multiple of 4
                            // right pad the final u32 with 0s.
                            if num < 4 {
                                u32_buff.resize(4, 0_u8);
                            }
                            let val = u32::from_be_bytes([
                                u32_buff[0],
                                u32_buff[1],
                                u32_buff[2],
                                u32_buff[3],
                            ]);
                            (checksum, _) = checksum.overflowing_add(val);
                        }
                    }
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
        NativeFileStore::new(tempdir_fixture.path())
    }

    #[rstest]
    fn create_file(test_filestore: &NativeFileStore) {
        let path = Path::new("junk.txt");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);
        assert!(full_path.exists())
    }

    #[rstest]
    fn delete_file(test_filestore: &NativeFileStore) {
        let path = Path::new("junk.txt");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);
        assert!(full_path.exists());

        test_filestore.delete_file(path).unwrap();

        assert!(!full_path.exists())
    }

    #[rstest]
    fn rename_file(test_filestore: &NativeFileStore) {
        let path = Path::new("junk.txt");
        let new_path = Path::new("new.dat");

        test_filestore.create_file(path).unwrap();

        let full_path = test_filestore.get_native_path(path);
        assert!(full_path.exists());

        test_filestore.rename_file(path, new_path).unwrap();

        assert!(!path.exists());
        assert!(!new_path.exists())
    }

    #[rstest]
    fn append_file(test_filestore: &NativeFileStore) {
        let mut options = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .to_owned();
        let path = "test_path1.txt";
        {
            let mut file = test_filestore.open(path, &mut options).unwrap();
            file.write_all("test text".as_bytes()).unwrap();
        }

        let path2 = "test_path2.txt";
        {
            let mut file = test_filestore.open(path2, &mut options).unwrap();
            file.write_all("new text".as_bytes()).unwrap();
        }
        let expected = "test textnew text".to_owned();

        test_filestore.append_file(path, path2).unwrap();

        let mut recovered_text = String::new();
        test_filestore
            .open(path, OpenOptions::new().read(true))
            .unwrap()
            .read_to_string(&mut recovered_text)
            .unwrap();

        assert_eq!(expected, recovered_text)
    }

    #[rstest]
    fn replace_file(test_filestore: &NativeFileStore) {
        let mut options = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .to_owned();
        let path = "test_path1.txt";
        {
            let mut file = test_filestore.open(path, &mut options).unwrap();
            file.write_all("test text".as_bytes()).unwrap();
        }

        let path2 = "test_path2.txt";
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
        let dir_path = Path::new("/help");
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
        file.seek(SeekFrom::Start(0))?;
        let mut recovered_text = String::new();
        file.read_to_string(&mut recovered_text)?;

        assert_eq!("hello, world!".to_owned(), recovered_text);
        Ok(())
    }

    #[rstest]
    fn get_filesize(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        let input_text = "Hello, world!";
        let expected = input_text.as_bytes().len() as u64;
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
    fn listdir(test_filestore: &NativeFileStore) -> FileStoreResult<()> {
        test_filestore.create_directory("listing")?;
        let basepath = Path::new("listing");

        let input_text = "Hello, world!";
        let input_text2 = "A longer string of text for this file.\n";

        let mut dir_size = HashMap::<&str, u64>::new();

        let mut timings = HashMap::<&str, u64>::new();

        for dirname in ["one", "two", "three"] {
            let dname = basepath.join(dirname);
            test_filestore.create_directory(&dname)?;
            let meta = fs::metadata(test_filestore.get_native_path(dname))
                .expect("No directorr for metadata.");
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
            dir = test_filestore.get_native_path(basepath).display(),
            t1 = timings.get("one").unwrap(),
            t2 = timings.get("three").unwrap(),
            t3 = timings.get("two").unwrap(),
            t4 = timings.get("new.dat").unwrap(),
            t5 = timings.get("test.txt").unwrap(),
            s1 = dir_size.get("one").unwrap(),
            s2 = dir_size.get("three").unwrap(),
            s3 = dir_size.get("two").unwrap(),
            s4 = input_text2.as_bytes().len(),
            s5 = input_text.as_bytes().len(),
        );
        let listing = test_filestore.list_directory("listing")?;
        assert_eq!(expected_listing, listing);
        Ok(())
    }

    #[rstest]
    fn checksum(
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
}
