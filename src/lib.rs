extern crate base64;
extern crate byteorder;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, Cursor, SeekFrom};
use std::path::PathBuf;

pub enum Decoded {
    Item(Item),
    TruncatedItem(TruncatedItem),
}

#[derive(Debug)]
pub struct Item {
    pub id: u32,
    pub data: Vec<u8>,
}

pub struct TruncatedItem {
    pub data: Vec<u8>,
}

pub struct Reader {
    path_buf: PathBuf,
}

type RecordId = u32;

const MARKER_ESCAPE: u8 = '\\' as u8;
const MARKER_SEPARATOR: u8 = '\n' as u8;
const MARKER_SEPARATOR_REMAP: u8 = '$' as u8;
const MARKER_FAIL: u8 = '-' as u8;
const MARKER_FAIL_REMAP: u8 = '.' as u8;

impl Reader {
    pub fn new<S: AsRef<str>>(path: S) -> io::Result<Reader> {
        let path_buf = PathBuf::from(path.as_ref());

        Ok(Reader { path_buf })
    }

    /// Returns an `Iterator` of items stored on the queue. This skips over
    /// items that were only partially written due to crash or power loss,
    ///
    /// If an offset is provided, streaming will resume from that position.
    pub fn stream(&self, offset: Option<u64>) -> io::Result<impl Iterator<Item = io::Result<Item>>> {
        let file = self.open_file()?;
        let buf_reader = BufReader::new(file);

        let iterator = buf_reader
            .split(MARKER_SEPARATOR)
            .map(|p| p.and_then(Self::parse_item))
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
    ///
    /// If an offset is provided, streaming will resume from that position.
    pub fn stream_with_truncated(&self, offset: Option<u64>) -> io::Result<impl Iterator<Item = io::Result<Decoded>>> {
        let file = self.open_file()?;
        let buf_reader = BufReader::new(file);

        let iterator = buf_reader
            .split(MARKER_SEPARATOR)
            .map(|p| p.and_then(Self::parse_item));

        Ok(iterator)
    }

    /// Opens a file for reading
    fn open_file(&self) -> io::Result<File> {
        // @TODO consider removing the write(true) -- it's only needed for create(true).
        OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .open(&self.path_buf)
    }

    /// Parses an item that was read off of the disk.
    fn parse_item(mut data: Vec<u8>) -> io::Result<Decoded> {
        let len = data.len();
        let id_length = 4;

        let mut escaped = false;
        let mut i = 0;
        let mut t = 0;

        // First, decode our data in place

        while i < len {
            let byte = data[i];

            if escaped && byte == MARKER_FAIL_REMAP {
                data[i - t] = MARKER_FAIL;
                escaped = false;
            } else if escaped && byte == MARKER_SEPARATOR_REMAP {
                data[i - t] = MARKER_SEPARATOR;
                escaped = false;
            } else if escaped && byte == MARKER_ESCAPE {
                data[i - t] = MARKER_ESCAPE;
                escaped = false;
            } else if escaped {
                return Err(
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("cannot parse file, invalid byte {} after escape", byte),
                    )
                );
            } else if byte == MARKER_ESCAPE {
                escaped = true;
                t += 1;
            } else {
                data[i - t] = data[i];
            }

            i += 1;
        }

        let new_length = len - id_length - t;

        if new_length < id_length {
            return Err(
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "cannot parse file, error reading id",
                )
            );
        }

        if len > 0 && data[len -1] == MARKER_FAIL {
            return Ok(Decoded::TruncatedItem(TruncatedItem { data }));
        }

        // the ID is now the first four bytes, big endian, so extract it

        let id = {
            let mut id_cursor = Cursor::new(&data[0 .. id_length]);
            id_cursor.read_u32::<BigEndian>()?
        };

        // finally, shift everything over and truncate the item's vector.

        let mut i = id_length;

        while i < len {
            data[i - id_length] = data[i];
            i += 1;
        }

        data.truncate(new_length);

        Ok(Decoded::Item(Item { id, data }))
    }
}

pub struct Writer {
    buffer: BufWriter<File>,
    id_buffer: Vec<u8>,
    item_buffer: [u8; 2],
    next_id: u32,
}

impl Writer {
    /// Creates a new Writer from the given file.
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

            let length = if eof[0] == MARKER_SEPARATOR as u8 {
                length
            } else {
                // We must have crashed before flushing to disk
                // Thus, we have no choice but to append a marker
                // and the newline. If this crashes, on the next
                // recovery we'd simply have n markers. Since all
                // items are encoded according to our scheme, - and \n cannot be
                // part of the payload.
                // Since we're append only, this could allow other
                // processes to inspect failed elements, i.e. we
                // don't actively remove any data.
                // Note that we append MARKER_FAIL twice as its possible
                // we crashed with the last value being an escape character,
                // so the first could potentially be escaped.
                file.write_all(&[MARKER_FAIL as u8, MARKER_FAIL as u8, MARKER_SEPARATOR as u8])?;
                file.flush()?;
                length + 2
            };

            last_id(&mut file, length)?
        } else {
            None
        };

        let buffer = BufWriter::new(file);
        let id_buffer = Vec::with_capacity(4); // FIXME can id_buffer by a slice? don't see how with byteorder crate, see #99

        // Upto 2 bytes are needed for each step -- the relevant data and possibly
        // an escape.
        let item_buffer: [u8; 2] = [0; 2];

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
        // the worst case is that every byte is escaped, so we need
        // twice the buffer size

        if data.len() > self.item_buffer.len() / 2 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "data cannot fit into buffer",
            ));
        }

        self.id_buffer.clear();
        self.id_buffer.write_u32::<BigEndian>(self.next_id)?;

        let bytes = [&self.id_buffer[0 .. 4], data];

        let mut i = 0;

        while i < bytes.len() {
            let bs = bytes[i];

            for byte in bs {
                match *byte {
                    MARKER_ESCAPE => {
                        self.item_buffer[0] = MARKER_ESCAPE;
                        self.item_buffer[1] = MARKER_ESCAPE;
                        self.buffer.write_all(&self.item_buffer[0 .. 2])?;
                    }

                    MARKER_SEPARATOR => {
                        self.item_buffer[0] = MARKER_ESCAPE;
                        self.item_buffer[1] = MARKER_SEPARATOR_REMAP;
                        self.buffer.write_all(&self.item_buffer[0 .. 2])?;
                    }

                    MARKER_FAIL => {
                        self.item_buffer[0] = MARKER_ESCAPE;
                        self.item_buffer[1] = MARKER_FAIL_REMAP;
                        self.buffer.write_all(&self.item_buffer[0 .. 2])?;
                    }

                    other => {
                        self.item_buffer[0] = other;
                        self.buffer.write_all(&self.item_buffer[0 .. 1])?;
                    }
                };
            }
            i += 1;
        }

        self.buffer.write_all(&[MARKER_SEPARATOR as u8])?;

        self.next_id += 1;

        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.next_id == 1
    }

    /// Determines the last id that was written, or `None` if
    /// empty.
    pub fn last_id(&self) -> Option<u32> {
        if self.is_empty() {
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

/// Given and open file and its total length, extract the last id
/// that was written. This should perform well for the average case,
/// but in the event of repeated power outages, with no writes since,
/// this could perform quite slowly as it needs to read the file
/// backwards and that doesn't work well with file I/O in general.
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
                .split(|b| b == &(MARKER_SEPARATOR as u8))
                .rev()
                .peekable();

            while let Some(l) = iterator.next() {
                if iterator.peek().is_none() {
                    next_current_line = l.to_vec();
                } else if l.ends_with(&[MARKER_FAIL as u8]) {
                    bad_ids += 1;
                } else if l.len() >= 4 {
                    let id_data = &l[0 .. 4];
                    let mut id_cursor = Cursor::new(id_data);
                    let id = id_cursor.read_u32::<BigEndian>()?;
                    let next_id = id + bad_ids;

                    return Ok(Some(next_id));
                } else if i > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "cannot parse file, missing data",
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

#[test]
fn test_read() {
    return;
    use std::str;
    let reader = Reader::new("/home/longshorej/test.what").unwrap();

    let mut iter = reader.stream(None).unwrap();

    let mut i = 0;
    while let Some(item) = iter.next() {
        //println!("item: {:?}", str::from_utf8(&item.unwrap().data).unwrap());
        i += 1;
    }

    println!("read: {}", i);
}

#[test]
fn test() {
    //return;
    let mut appender = Writer::new(&PathBuf::from("/home/longshorej/test.what")).unwrap();

    let mut i = 0;

    while i < 20_000_000 {
        let message = format!("the quick brown fox jumped over the lazy dog, -\n #{}", i);
        let data = message.as_bytes();
        appender.append(&data).unwrap();
        i += 1;
    }

    appender.sync().unwrap();
}

