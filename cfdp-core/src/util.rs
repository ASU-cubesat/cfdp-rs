use std::{
    ffi::{OsStr, OsString},
    io::{Error as IOError, Read},
    num::TryFromIntError,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

#[cfg(windows)]
use std::os::windows::ffi::{OsStrExt, OsStringExt};

type PathEncodeResult<T> = Result<T, PathEncodeError>;
#[derive(Debug)]
pub enum PathEncodeError {
    IntConversion(TryFromIntError),
    IO(IOError),
}
impl From<TryFromIntError> for PathEncodeError {
    fn from(err: TryFromIntError) -> Self {
        Self::IntConversion(err)
    }
}
impl From<IOError> for PathEncodeError {
    fn from(err: IOError) -> Self {
        Self::IO(err)
    }
}

impl std::fmt::Display for PathEncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IntConversion(error) => write!(
                f,
                "Path length is too long.
                Can only support paths with a byte with less than 256 bytes. {}",
                error
            ),
            Self::IO(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for PathEncodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::IntConversion(source) => Some(source),
            Self::IO(source) => Some(source),
        }
    }
}

// this trait can be used to coerce an OSstr into a Bytes array
// adapted from https://stackoverflow.com/a/59224987
pub(crate) trait PathToBytes {
    fn to_vec(&self) -> Vec<u8>;

    fn encode(&self) -> PathEncodeResult<Vec<u8>> {
        let vec = self.to_vec();

        // we have a hard limit on 255 bytes because of
        // how Length Value types work in CFDP
        let mut buff = vec![vec.len().try_into()?];

        buff.extend(vec);
        Ok(buff)
    }

    fn decode<R: Read>(buffer: &mut R) -> PathEncodeResult<PathBuf>;
}
#[cfg(unix)]
impl<P: AsRef<Path>> PathToBytes for P {
    fn to_vec(&self) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];
        buff.extend(self.as_ref().as_os_str().as_bytes());
        buff
    }

    fn decode<R: Read>(buffer: &mut R) -> PathEncodeResult<PathBuf> {
        let mut u8buff = [0_u8; 1];
        buffer.read_exact(&mut u8buff)?;
        let os_str_name: OsString = {
            let len = u8buff[0];
            let mut temp = vec![0_u8; len.into()];
            buffer.read_exact(&mut temp)?;
            OsStr::from_bytes(temp.as_slice()).to_owned()
        };
        Ok(os_str_name.into())
    }
}

#[cfg(windows)]
impl<P: AsRef<Path>> PathToBytes for P {
    fn to_vec(&self) -> Vec<u8> {
        let mut buff: Vec<u8> = vec![];
        buff.extend(
            self.as_ref()
                .as_os_str()
                .encode_wide()
                .map(|b| {
                    let b = b.to_be_bytes();
                    b.get(0).map(|s| *s).into_iter().chain(b.get(1).map(|s| *s))
                })
                .flatten(),
        );

        buff
    }

    fn decode<R: Read>(buffer: &mut R) -> PathEncodeResult<PathBuf> {
        let mut u8buff = [0_u8; 1];
        buffer.read_exact(&mut u8buff)?;
        let os_str_name: OsString = {
            let len = u8buff[0];
            let temp = vec![0_u8; len.into()];
            buffer.read_exact(&mut temp)?;
            let wide_buffer = buffer
                .as_slice()
                .chunks_exact(2)
                .map(|val| u16::from_be_bytes([val[0], val[1]]))
                .collect();
            OsString::from_wide(&wide_buffer[..])
        };
        Ok(os_str_name.into())
    }
}

#[cfg(test)]
mod test {

    use super::*;

    use dirs::home_dir;
    use rstest::rstest;

    #[rstest]
    fn encode_pathbuf(
        #[values("", home_dir().unwrap_or_else(PathBuf::new), "/tmp/test.txt")] expected: PathBuf,
    ) {
        let buffer = expected.encode().unwrap();
        let recovered = PathBuf::decode(&mut &buffer[..]).unwrap();

        assert_eq!(expected, recovered)
    }
}
