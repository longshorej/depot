use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::PathBuf;

/// The main item type for a Section. An item has an id
/// which can be used to efficiently resume reading
/// and a Vec<u8> containing the data for that item.
#[derive(Debug)]
pub struct Item {
    pub id: u32,
    pub data: Vec<u8>,
    pub known_eof: bool,
}

/// A TruncatedItem represents data that was potentially
/// truncated when written. The data field of this struct
/// is not decoded and will end with two fail markers (45)
#[derive(Debug)]
pub struct TruncatedItem {
    pub id: u32,
    pub data: Vec<u8>,
    pub known_eof: bool,
}

/// A container for the two possible types of items,
/// those that were successfully written and those that
/// were potentially truncated.
#[derive(Debug)]
pub enum Decoded {
    Item(Item),
    TruncatedItem(TruncatedItem),
}

/// A chunk size for reading in bytes.
const READ_CHUNK_SIZE: u32 = 8192;

/// A chunk size for writing in bytes.
const WRITE_CHUNK_SIZE: u32 = 8192;

/// An absolute max size for files on disk.
/// Exceeding this value results in failure, but
/// this "should" never happen unless there's
/// external interference. This is max file size
/// plus three times the maximum item size,
/// which remains well under the last extra bit,
/// and provides enough overhead to deal with
/// encoding overheads. The main idea here is
/// to guarantee that an id can be represented
/// by a 32bit SIGNED integer, not using the
/// upper most bit, to make compat with java
/// and other environments easier.
const FAIL_FILE_SIZE: u32 = 2_147_483_647;

/// Represents the special values that are escaped
/// and remapped as part of the on-disk format.
const MARKER_ESCAPE: u8 = '\\' as u8;
const MARKER_SEPARATOR: u8 = '\n' as u8;
const MARKER_SEPARATOR_REMAP: u8 = '$' as u8;
const MARKER_FAIL: u8 = '-' as u8;
const MARKER_FAIL_REMAP: u8 = '.' as u8;

/// If a file is this size or larger, the section
/// be considered full and no more writes will be
/// allowed. Note that this means that the size of
/// a file may exceed this by the maximum item size.

// @TODO make these configurable for testing
const MAX_FILE_SIZE1: u32 = FAIL_FILE_SIZE - (3 * MAX_ITEM_SIZE);
const MAX_FILE_SIZE: u32 = 8388608;

/// The maximum size that an item on disk can be.
const MAX_ITEM_SIZE: u32 = 65_536;

#[derive(Debug)]
pub struct Section {
    reader: SectionReader,
    writer: SectionWriter,
}

/// A section is used to store items on disk and retrieve them.
///
/// Since a section can be become full, it is recommended to use
/// the higher level interfaces that manage the creation of
/// new sections for you, notably a Queue.
impl Section {
    /// Determines if a theoretical offset is past the end of this
    /// section.
    pub fn is_eof(offset: u32) -> bool {
        offset >= MAX_FILE_SIZE
    }

    /// Opens the section, creating it if it doesn't exist.
    pub fn new(path: &PathBuf) -> io::Result<Section> {
        let writer = SectionWriter::new(path)?;
        let reader = SectionReader::new(path);

        Ok(Section { reader, writer })
    }

    /// Append the given data to the file.
    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        self.writer.append(data)
    }

    /// Determines if the section is empty, i.e. no items
    /// have been written to it.
    pub fn is_empty(&self) -> bool {
        self.writer.is_empty()
    }

    /// Determines if the section is full, i.e. it will not
    /// accept any more items.
    pub fn is_full(&self) -> bool {
        self.writer.is_full()
    }

    /// Determines the last id that was written, or `None` if
    /// empty.
    pub fn last_id(&mut self) -> Option<u32> {
        self.writer.last_id()
    }

    /// Forces all items that have been appended to be written
    /// out to disk.
    pub fn sync(&mut self) -> io::Result<()> {
        self.writer.sync()
    }

    /// Iterate over items stored in this section, starting at the
    /// specified id (inclusive, if provided).
    ///
    /// Note that this skips over items that were only partially
    /// written due to crash or power loss, which is typically
    /// the preferred behavior.
    pub fn stream(&self, id: Option<u32>) -> io::Result<impl Iterator<Item = io::Result<Item>>> {
        self.reader.stream(id)
    }

    /// Iterate over items stored in this section, starting at the
    /// specified id (inclusive, if provided).
    ///
    /// Note that this DOES NOT skip over items that may have been
    /// partially written. This is not a usual mode of operation, but
    /// may be useful for some systems.
    pub fn stream_with_truncated(
        &self,
        id: Option<u32>,
    ) -> io::Result<impl Iterator<Item = io::Result<Decoded>>> {
        self.reader.stream_with_truncated(id)
    }
}

#[derive(Debug)]
pub(crate) struct SectionReader {
    path_buf: PathBuf,
}

impl SectionReader {
    pub(crate) fn new(path: &PathBuf) -> SectionReader {
        let path_buf = PathBuf::from(path);

        SectionReader { path_buf }
    }

    pub(crate) fn stream(
        &self,
        id: Option<u32>,
    ) -> io::Result<impl Iterator<Item = io::Result<Item>>> {
        let iterator = self.stream_with_truncated(id)?.filter_map(|i| match i {
            Ok(Decoded::Item(i)) => Some(Ok(i)),
            Ok(Decoded::TruncatedItem(_)) => None,
            Err(e) => Some(Err(e)),
        });

        Ok(iterator)
    }

    pub(crate) fn stream_with_truncated(
        &self,
        id: Option<u32>,
    ) -> io::Result<impl Iterator<Item = io::Result<Decoded>>> {
        let mut file = self.open_file()?;

        // @FIXME should have a better error message so the user knows what's happening
        let mut always_fail = false;

        let mut position = if let Some(requested_id) = id {
            file.seek(SeekFrom::Start(requested_id as u64))?;
            requested_id
        } else {
            0
        };

        let buf_reader = BufReader::with_capacity(READ_CHUNK_SIZE as usize, file);

        let iterator = SplitWithCarry {
            buf: buf_reader,
            carry: None,
        };

        let iterator = iterator.map(move |p| {
            if always_fail {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "a previous error has halted further execution",
                ))
            } else {
                match p {
                    Ok(item) => {
                        let id = item.len();

                        let result = if id > MAX_ITEM_SIZE as usize {
                            always_fail = true;

                            Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("item at position {} exceeds the maximum length", position),
                            ))
                        } else {
                            position += 1 + id as u32;
                            let known_eof = position >= MAX_FILE_SIZE;
                            Self::parse_item(position, item, known_eof)
                        };

                        result
                    }

                    Err(e) => {
                        always_fail = true;
                        Err(e)
                    }
                }
            }
        });

        Ok(iterator)
    }

    fn open_file(&self) -> io::Result<File> {
        OpenOptions::new().read(true).open(&self.path_buf)
    }

    fn parse_item(id: u32, mut data: Vec<u8>, known_eof: bool) -> io::Result<Decoded> {
        let len = data.len();

        let mut escaped = false;
        let mut i = 0;
        let mut t = 0;

        // Decode our data in place

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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("cannot parse file, invalid byte {} after escape", byte),
                ));
            } else if byte == MARKER_ESCAPE {
                escaped = true;
                t += 1;
            } else {
                data[i - t] = data[i];
            }

            i += 1;
        }

        let new_length = len - t;

        // Determine if this was a truncated item, which is indicated by its last byte.

        if len > 1 && data[len - 1] == MARKER_FAIL {
            // this was a truncated record that was repaired
            Ok(Decoded::TruncatedItem(TruncatedItem {
                id,
                data,
                known_eof,
            }))
        } else {
            // we have a full record, so truncate the vector (removing control/escape chars)
            // and move on
            data.truncate(new_length);

            Ok(Decoded::Item(Item {
                id,
                data,
                known_eof,
            }))
        }
    }
}

pub struct SplitWithCarry<B> {
    buf: B,
    carry: Option<Vec<u8>>,
}

/// An iterator that works just like split, except
/// when reaching EOF, if the separator character
/// is not found, None is returned and the data
/// is "carried" over for the next call.
impl<B: BufRead> Iterator for SplitWithCarry<B> {
    type Item = io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<io::Result<Vec<u8>>> {
        let mut buf = Vec::new();

        if let Some(ref mut c) = self.carry {
            buf.append(c);
        }

        self.carry = None;

        match self.buf.read_until(MARKER_SEPARATOR, &mut buf) {
            Ok(0) => {
                if !buf.is_empty() {
                    self.carry = Some(buf);
                }

                None
            }
            Ok(_n) => {
                if buf[buf.len() - 1] == MARKER_SEPARATOR {
                    buf.pop();
                    Some(Ok(buf))
                } else {
                    self.carry = Some(buf);
                    None
                }
            }
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Debug)]
struct SectionWriter {
    buffer: BufWriter<File>,
    item_buffer: [u8; 2],
    last_id: Option<u32>,
    position: u32,
}

impl SectionWriter {
    fn new(path: &PathBuf) -> io::Result<SectionWriter> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .write(true)
            .read(true)
            .open(path)?;

        let meta = file.metadata()?;

        let length = meta.len();

        if length > FAIL_FILE_SIZE as u64 {
            // this should be a very rare condition,
            // but it's possible if another process
            // or user has tampered with data
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "file size exceeds maximum",
            ));
        }

        let position = if length > 0 {
            let mut buf = vec![0u8; 3];
            file.seek(SeekFrom::Start(length - 1))?;
            file.read_exact(&mut buf[0..1])?;

            let length = if buf[0] == MARKER_SEPARATOR as u8 {
                length
            } else {
                // We must have crashed before flushing to disk
                // Determine if we need to append the fail markers
                // -- two are needed incase we failed directly after
                // writing an escape character.
                //
                // If the last two characters of the file are the
                // fail marker, then we just need to append the newline.
                // This bounds the number of fail markers that will ever
                // appear at the end of a truncated record.
                //
                // This fairly simple algorithm is possible because \n
                // cannot be part of the payload, so we do not need to
                // track control characters etc.
                // Note that we don't do anything about the end of record
                // marker here as we are indeed truncated.

                let write_marker = if length > 2 {
                    file.seek(SeekFrom::Start(length - 3))?;
                    file.read_exact(&mut buf[0..3])?;
                    buf[0] != MARKER_FAIL as u8 || buf[1] != MARKER_FAIL as u8
                } else {
                    true
                };

                if write_marker {
                    file.write_all(&[
                        MARKER_FAIL as u8,
                        MARKER_FAIL as u8,
                        MARKER_SEPARATOR as u8,
                    ])?;
                    file.flush()?;
                    length + 3
                } else {
                    file.write_all(&[MARKER_SEPARATOR as u8])?;
                    file.flush()?;
                    length + 1
                }
            };

            // length as u32 cannot overflow -- look at the validation
            // earlier in this fn
            length as u32
        } else {
            0
        };

        file.seek(SeekFrom::Start(position as u64))?;

        let last_id = last_id(&mut file, position as u32)?;

        let buffer = BufWriter::with_capacity(WRITE_CHUNK_SIZE as usize, file);

        // Upto 2 bytes are needed for each step -- the relevant data and possibly
        // an escape.
        let item_buffer: [u8; 2] = [0; 2];

        Ok(SectionWriter {
            buffer,
            item_buffer,
            last_id,
            position,
        })
    }

    fn append(&mut self, data: &[u8]) -> io::Result<()> {
        if data.len() > MAX_ITEM_SIZE as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "item exceeds max item size",
            ));
        }

        if self.is_full() {
            return Err(io::Error::new(io::ErrorKind::Other, "section is full"));
        }

        let next_id = self.position;

        for byte in data {
            match *byte {
                MARKER_ESCAPE => {
                    self.item_buffer[0] = MARKER_ESCAPE;
                    self.item_buffer[1] = MARKER_ESCAPE;
                    self.buffer.write_all(&self.item_buffer[0..2])?;
                    self.position += 2;
                }

                MARKER_SEPARATOR => {
                    self.item_buffer[0] = MARKER_ESCAPE;
                    self.item_buffer[1] = MARKER_SEPARATOR_REMAP;
                    self.buffer.write_all(&self.item_buffer[0..2])?;
                    self.position += 2;
                }

                MARKER_FAIL => {
                    self.item_buffer[0] = MARKER_ESCAPE;
                    self.item_buffer[1] = MARKER_FAIL_REMAP;
                    self.buffer.write_all(&self.item_buffer[0..2])?;
                    self.position += 2;
                }

                other => {
                    self.item_buffer[0] = other;
                    self.buffer.write_all(&self.item_buffer[0..1])?;
                    self.position += 1;
                }
            };
        }

        self.buffer.write_all(&[MARKER_SEPARATOR as u8])?;
        self.position += 1;
        self.last_id = Some(next_id);

        if self.is_full() {
            self.sync()?;
        }

        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.last_id == None
    }

    fn is_full(&self) -> bool {
        self.position >= MAX_FILE_SIZE
    }

    fn last_id(&mut self) -> Option<u32> {
        self.last_id
    }

    fn sync(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }
}

/// Given an open file and its total length, extract the last id
/// that was written. Note that this by design only works with
/// 32bit unsigned integers in length, so the caller must validate
/// this before hand.
fn last_id(file: &mut File, length: u32) -> io::Result<Option<u32>> {
    let mut buf = vec![0u8; READ_CHUNK_SIZE as usize];
    let mut total = 0;
    let mut items = 0;

    while total < length {
        let pos = length - total;

        let starting_at = if pos < READ_CHUNK_SIZE {
            0
        } else {
            pos - READ_CHUNK_SIZE
        };

        let bytes_to_read = pos - starting_at;
        file.seek(SeekFrom::Start(starting_at as u64))?;
        file.read_exact(&mut buf[0..bytes_to_read as usize])?;
        total += bytes_to_read;

        let mut i = 1;

        while i < bytes_to_read {
            let p = bytes_to_read - i;

            if buf[p as usize] == MARKER_SEPARATOR && items > 0 {
                return Ok(Some(pos - p));
            } else if buf[p as usize] == MARKER_SEPARATOR {
                items += 1;
            }

            i += 1;
        }
    }

    if length == 0 {
        Ok(None)
    } else if items == 0 {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "file missing record separator",
        ))
    } else {
        Ok(Some(0))
    }
}

#[cfg(test)]
mod tests {
    // @TODO implement these. there is some coverage implicitly via queue tests
    // @TODO but that's not good enough.
}
