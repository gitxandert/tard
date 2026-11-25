// Wire format between `tard server ...` and the daemon, over localhost TCP.
// Request: token, then the client's argv (paths already made absolute).
// Response: frames of stdout/stderr text, ending with the exit code.
// Every byte string is a u32 little-endian length followed by the bytes.
use std::io::{self, Read, Write};

const MAX_BYTES: usize = 16 << 20;
const MAX_ARGS: usize = 4096;

pub enum Frame {
    Out(Vec<u8>),
    Err(Vec<u8>),
    Exit(i32),
}

pub fn write_request<W: Write>(w: &mut W, token: &[u8], args: &[Vec<u8>]) -> io::Result<()> {
    write_bytes(w, token)?;
    w.write_all(&(args.len() as u32).to_le_bytes())?;
    for a in args {
        write_bytes(w, a)?;
    }
    w.flush()
}

pub fn read_request<R: Read>(r: &mut R) -> io::Result<(Vec<u8>, Vec<Vec<u8>>)> {
    let token = read_bytes(r)?;
    let argc = read_u32(r)? as usize;
    if argc > MAX_ARGS {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "too many arguments"));
    }
    let args = (0..argc).map(|_| read_bytes(r)).collect::<io::Result<_>>()?;
    Ok((token, args))
}

pub fn write_frame<W: Write>(w: &mut W, frame: &Frame) -> io::Result<()> {
    match frame {
        Frame::Out(b) => { w.write_all(b"o")?; write_bytes(w, b)?; }
        Frame::Err(b) => { w.write_all(b"e")?; write_bytes(w, b)?; }
        Frame::Exit(c) => { w.write_all(b"x")?; write_bytes(w, &c.to_le_bytes())?; }
    }
    w.flush()
}

pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Frame> {
    let mut tag = [0u8; 1];
    r.read_exact(&mut tag)?;
    let body = read_bytes(r)?;
    match &tag {
        b"o" => Ok(Frame::Out(body)),
        b"e" => Ok(Frame::Err(body)),
        b"x" => {
            let code: [u8; 4] = body.try_into()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad exit frame"))?;
            Ok(Frame::Exit(i32::from_le_bytes(code)))
        }
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "unknown frame")),
    }
}

fn write_bytes<W: Write>(w: &mut W, b: &[u8]) -> io::Result<()> {
    w.write_all(&(b.len() as u32).to_le_bytes())?;
    w.write_all(b)
}

fn read_bytes<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
    if len > MAX_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut b = vec![0u8; len];
    r.read_exact(&mut b)?;
    Ok(b)
}

fn read_u32<R: Read>(r: &mut R) -> io::Result<u32> {
    let mut b = [0u8; 4];
    r.read_exact(&mut b)?;
    Ok(u32::from_le_bytes(b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trip() {
        let mut buf = Vec::new();
        let args = vec![b"stop".to_vec(), b"a\nb\0c".to_vec()];
        write_request(&mut buf, b"tok", &args).unwrap();
        let (tok, got) = read_request(&mut buf.as_slice()).unwrap();
        assert_eq!(tok, b"tok");
        assert_eq!(got, args);
    }

    #[test]
    fn frames_round_trip() {
        let mut buf = Vec::new();
        write_frame(&mut buf, &Frame::Out(b"hi".to_vec())).unwrap();
        write_frame(&mut buf, &Frame::Exit(-3)).unwrap();
        let mut r = buf.as_slice();
        assert!(matches!(read_frame(&mut r).unwrap(), Frame::Out(b) if b == b"hi"));
        assert!(matches!(read_frame(&mut r).unwrap(), Frame::Exit(-3)));
    }
}
