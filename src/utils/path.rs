// How paths are stored in an archive: raw OS bytes on Unix (any filename
// round-trips); UTF-8 with '/' separators elsewhere, so Windows archives
// extract correctly on Unix and vice versa.
use std::{
    io,
    path::{Path, PathBuf},
};

#[cfg(unix)]
pub fn path_to_bytes(path: &Path) -> io::Result<Vec<u8>> {
    use std::os::unix::ffi::OsStrExt;
    Ok(path.as_os_str().as_bytes().to_vec())
}

#[cfg(not(unix))]
pub fn path_to_bytes(path: &Path) -> io::Result<Vec<u8>> {
    let s = path.to_str().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "path is not valid Unicode")
    })?;
    Ok(s.replace('\\', "/").into_bytes())
}

#[cfg(unix)]
pub fn bytes_to_path(bytes: Vec<u8>) -> io::Result<PathBuf> {
    use std::os::unix::ffi::OsStringExt;
    Ok(PathBuf::from(std::ffi::OsString::from_vec(bytes)))
}

#[cfg(not(unix))]
pub fn bytes_to_path(bytes: Vec<u8>) -> io::Result<PathBuf> {
    let s = String::from_utf8(bytes).map_err(|_| {
        io::Error::new(io::ErrorKind::InvalidData, "archived path is not valid UTF-8")
    })?;
    Ok(s.split('/').collect())
}
