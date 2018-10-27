extern crate base64;
extern crate byteorder;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Cursor, SeekFrom};
use std::path::PathBuf;

const MARKER_FAILURE: char = '-';
const MARKER_FIELD: char = '\\';
const MARKER_NEWLINE: char = '\n';

pub enum Decoded {
    Item(Item),
    TruncatedItem(TruncatedItem),
}

#[derive(Debug)]
pub struct Item {
    id: u32,
    data: Vec<u8>,
}

pub struct TruncatedItem {
    data: Option<Vec<u8>>,
}

pub struct Reader {
    path_buf: PathBuf,
}

impl Reader {
    pub fn new<S: AsRef<str>>(path: S) -> io::Result<Reader> {
        let path_buf = PathBuf::from(path.as_ref());

        Ok(Reader { path_buf })
    }

    /// Returns an `Iterator` of items stored on the queue. This skips over
    /// items that were only partially written due to crash or power loss,
    pub fn stream(&self) -> io::Result<impl Iterator<Item = io::Result<Item>>> {
        let file = self.open_file()?;
        let buf_reader = BufReader::new(file);

        let iterator = buf_reader
            .lines()
            .map(|p| p.and_then(|l| Reader::parse_item(l, false)))
            .filter_map(|i| match i {
                Ok(Decoded::Item(i)) => Some(Ok(i)),
                Ok(Decoded::TruncatedItem(_)) => None,
                Err(e) => Some(Err(e)),
            });

        Ok(iterator)
    }

    /// Returns an `Iterator` of items stored on the queue, including
    /// those that were truncated due to crash or power loss.
    ///
    /// This is useful for e.g. replication systems that may be built
    /// on this queue as perhaps parttially written data is of use as
    /// well.
    pub fn stream_with_truncated(&self) -> io::Result<impl Iterator<Item = io::Result<Decoded>>> {
        let file = self.open_file()?;
        let buf_reader = BufReader::new(file);

        let iterator = buf_reader
            .lines()
            .map(|p| p.and_then(|l| Reader::parse_item(l, true)));

        Ok(iterator)
    }

    fn open_file(&self) -> io::Result<File> {
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&self.path_buf)
    }

    fn parse_item<S: AsRef<str>>(data: S, copy_truncated: bool) -> io::Result<Decoded> {
        let data = data.as_ref();
        let mut parts = data.split(MARKER_FIELD);

        let data_part = parts.next();
        let id_part = parts.next();
        let empty_part = parts.next();

        match (data_part, id_part, empty_part) {
            (Some(_), Some(id_bytes), None) if id_bytes.ends_with(MARKER_FAILURE) => {
                let item = if copy_truncated {
                    TruncatedItem {
                        data: Some(data.as_bytes().to_vec()),
                    }
                } else {
                    TruncatedItem { data: None }
                };

                Ok(Decoded::TruncatedItem(item))
            }

            (Some(data_bytes), Some(id_bytes), None) => {
                // @TODO FIXME unwraps

                let id_data = base64::decode(&id_bytes).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cannot parse file, error reading id",
                    )
                })?;

                let mut id_cursor = Cursor::new(id_data);
                let id = id_cursor.read_u32::<BigEndian>().unwrap();

                let data = base64::decode(&data_bytes).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cannot parse file, error reading data",
                    )
                })?;

                Ok(Decoded::Item(Item { id, data }))
            }

            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "cannot parse file, error reading data",
            )),
        }
    }
}

pub struct Writer {
    buffer: BufWriter<File>,
    id_buffer: Vec<u8>,
    item_buffer: [u8; 8192],
    next_id: u32,
}

impl Writer {
    /// Creates a new Writer from the given file.
    /// @TODO use multiple files to allow rewriting sections
    pub fn new(path: &PathBuf) -> io::Result<Writer> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .read(true)
            .open(path)?;

        let meta = file.metadata()?;

        let length = meta.len();

        let maybe_last_id = if length > 0 {
            file.seek(SeekFrom::Start(length - 1))?;
            let mut eof = vec![0u8; 1];
            file.read_exact(&mut eof)?;

            let length = if eof[0] == MARKER_NEWLINE as u8 {
                length
            } else {
                // We must have crashed before flushing to disk
                // Thus, we have no choice but to append a marker
                // and the newline. If this crashes, on the next
                // recovery we'd simply have n markers. Since all
                // items are base64 encoded, - and \n cannot be
                // part of the payload.
                // Since we're append only, this could allow other
                // processes to inspect failed elements, i.e. we
                // don't actively remove any data.
                file.write_all(&[MARKER_FAILURE as u8, MARKER_NEWLINE as u8])?;
                file.flush()?;
                length + 2
            };

            last_id(&mut file, length)?
        } else {
            None
        };

        let buffer = BufWriter::new(file);
        // FIXME can id_buffer by a slice? don't see how with byteorder crate, see #99
        let id_buffer = Vec::with_capacity(4);
        let item_buffer: [u8; 8192] = [0; 8192];

        let next_id = maybe_last_id.unwrap_or_else(|| 0) + 1;

        Ok(Writer {
            buffer,
            id_buffer,
            item_buffer,
            next_id,
        })
    }

    /// Append the given data to the file.
    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        // technically, 4/3 the size is all that's needed, but for
        // we simply require data to fit in half the buffer size
        // for simplicity

        if data.len() > self.item_buffer.len() / 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data cannot fit into buffer",
            ));
        }

        let bytes_written =
            base64::encode_config_slice(data, base64::STANDARD_NO_PAD, &mut self.item_buffer);

        self.id_buffer.clear();
        self.id_buffer.write_u32::<BigEndian>(self.next_id)?;

        self.item_buffer[bytes_written] = MARKER_FIELD as u8;

        let id_bytes_written = base64::encode_config_slice(
            &self.id_buffer,
            base64::STANDARD_NO_PAD,
            &mut self.item_buffer[bytes_written + 1..],
        );

        let bytes_slice = &self.item_buffer[0..bytes_written + 1 + id_bytes_written];

        self.buffer.write_all(bytes_slice)?;

        self.buffer.write_all(&[MARKER_NEWLINE as u8])?;

        self.next_id += 1;

        Ok(())
    }

    /// Determines the last id that was written
    pub fn last_id(&self) -> Option<u32> {
        if self.next_id == 1 {
            None
        } else {
            Some(self.next_id - 1)
        }
    }

    /// Forces a flush to disk
    pub fn sync(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }
}

#[test]
fn test_read() {
    let reader = Reader::new("/home/longshorej/test.what").unwrap();

    let mut iter = reader.stream().unwrap();

    let mut i = 0;
    while let Some(item) = iter.next() {
        //println!("{:?}", item);
        i += 1;
    }

    println!("read: {}", i);
}

//#[test]
fn test() {
    let mut appender = Writer::new(&PathBuf::from("/home/longshorej/test.what")).unwrap();

    let mut i = 0;
    let data = [1, 2, 3, 4];

    while i < 10_000_000 {
        appender.append(&data).unwrap();
        i += 1;
    }

    appender.sync().unwrap();
}

fn last_id(file: &mut File, length: u64) -> io::Result<Option<u32>> {
    let chunk_size = 8192;

    let mut bad_ids = 0;
    let mut buf = vec![0u8; chunk_size as usize];
    let mut total_bytes_read = 0;
    let mut current_line = vec![];

    while total_bytes_read != length {
        let pos = length - total_bytes_read;

        let starting_at = if pos < chunk_size {
            0
        } else {
            pos - chunk_size
        };

        let bytes_to_read = (pos - starting_at) as usize;

        file.seek(SeekFrom::Start(starting_at))?;

        let mut bytes_read = 0;

        while bytes_read < bytes_to_read {
            bytes_read += file.read(&mut buf[bytes_read..bytes_to_read])?;
        }

        current_line.splice(0..0, buf[..bytes_read].iter().cloned());

        let mut next_current_line = vec![];

        {
            let mut i = 0;
            let mut iterator = current_line
                .split(|b| b == &(MARKER_NEWLINE as u8))
                .rev()
                .peekable();

            while let Some(l) = iterator.next() {
                if iterator.peek().is_none() {
                    next_current_line = l.to_vec();
                } else if l.ends_with(&['-' as u8]) {
                    bad_ids += 1;
                } else if !l.is_empty() {
                    match l.split(|b| b == &(MARKER_FIELD as u8)).last() {
                        Some(bytes) => {
                            let id_data = base64::decode(&bytes).map_err(|_| {
                                io::Error::new(
                                    io::ErrorKind::InvalidData,
                                    "cannot parse file, error reading id",
                                )
                            })?;
                            let mut id_cursor = Cursor::new(id_data);
                            let id = id_cursor.read_u32::<BigEndian>()?;
                            let next_id = id + bad_ids;

                            return Ok(Some(next_id));
                        }

                        None => {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                "cannot parse file, line is missing field separator",
                            ));
                        }
                    }
                } else if i > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cannot parse file, empty line",
                    ));
                } else if !l.is_empty() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cannot parse file, missing newline",
                    ));
                }

                i += 1;
            }
        }

        current_line = next_current_line;
        total_bytes_read += bytes_read as u64;
    }

    Ok(if bad_ids == 0 { None } else { Some(bad_ids) })
}
