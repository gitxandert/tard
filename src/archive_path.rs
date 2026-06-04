use std::{
    io,
    path::{Component, Path, PathBuf},
};

pub(crate) fn write_archive_path(buf: &mut Vec<u8>, path: &Path) -> io::Result<()> {
    let header_start = buf.len();
    buf.extend_from_slice(&0u64.to_le_bytes());
    let payload_start = buf.len();

    match write_archive_path_payload(buf, path) {
        Ok(()) => {
            let path_len = (buf.len() - payload_start) as u64;
            buf[header_start..payload_start].copy_from_slice(&path_len.to_le_bytes());
            Ok(())
        }
        Err(e) => {
            buf.truncate(header_start);
            Err(e)
        }
    }
}

pub(crate) fn decode_archive_path(path_bytes: &[u8]) -> io::Result<PathBuf> {
    let path = std::str::from_utf8(path_bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "archive path is not UTF-8"))?;
    let mut decoded = PathBuf::new();

    for component in path.split('/') {
        validate_component(component, io::ErrorKind::InvalidData)?;
        decoded.push(component);
    }

    if decoded.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "archive path is empty",
        ));
    }

    Ok(decoded)
}

fn write_archive_path_payload(buf: &mut Vec<u8>, path: &Path) -> io::Result<()> {
    let mut wrote_component = false;

    for component in path.components() {
        let Component::Normal(component) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "archive paths must be relative normal paths",
            ));
        };
        let component = component.to_str().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "archive paths must be valid UTF-8",
            )
        })?;
        validate_component(component, io::ErrorKind::InvalidInput)?;

        if wrote_component {
            buf.push(b'/');
        }
        buf.extend_from_slice(component.as_bytes());
        wrote_component = true;
    }

    if !wrote_component {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "archive path is empty",
        ));
    }

    Ok(())
}

fn validate_component(component: &str, kind: io::ErrorKind) -> io::Result<()> {
    if component.is_empty() || component == "." || component == ".." {
        return Err(io::Error::new(
            kind,
            "archive path contains unsafe component",
        ));
    }
    if component.contains(['/', '\\', ':']) {
        return Err(io::Error::new(
            kind,
            "archive path contains a non-portable component",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_archive_path_into_existing_buffer() {
        let mut buf = vec![0xaa, 0xbb];

        write_archive_path(&mut buf, Path::new("root/sub/file.txt")).unwrap();

        assert_eq!(&buf[..2], &[0xaa, 0xbb]);
        let path_len = u64::from_le_bytes(buf[2..10].try_into().unwrap());
        assert_eq!(path_len, "root/sub/file.txt".len() as u64);
        assert_eq!(&buf[10..], b"root/sub/file.txt");
    }

    #[test]
    fn failed_write_restores_buffer() {
        let mut buf = vec![0xaa, 0xbb];

        let err = write_archive_path(&mut buf, Path::new("../file.txt")).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buf, vec![0xaa, 0xbb]);
    }

    #[test]
    fn decodes_archive_path_to_native_pathbuf() {
        let path = decode_archive_path(b"root/sub/file.txt").unwrap();
        let mut expected = PathBuf::new();
        expected.push("root");
        expected.push("sub");
        expected.push("file.txt");

        assert_eq!(path, expected);
    }

    #[test]
    fn rejects_unsafe_archive_payloads() {
        for payload in [
            b"".as_slice(),
            b"/root/file.txt",
            b"root//file.txt",
            b"root/./file.txt",
            b"root/../file.txt",
            b"C:/root/file.txt",
            b"root\\file.txt",
        ] {
            let err = decode_archive_path(payload).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn rejects_non_utf8_archive_payload() {
        let err = decode_archive_path(&[0xff]).unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(windows)]
    #[test]
    fn writes_windows_paths_with_archive_separators() {
        let mut buf = Vec::new();

        write_archive_path(&mut buf, Path::new(r"root\sub\file.txt")).unwrap();

        let path_len = u64::from_le_bytes(buf[..8].try_into().unwrap());
        assert_eq!(path_len, "root/sub/file.txt".len() as u64);
        assert_eq!(&buf[8..], b"root/sub/file.txt");
    }
}
