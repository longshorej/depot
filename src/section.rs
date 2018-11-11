use std::cmp;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, BufWriter, SeekFrom};
use std::path::PathBuf;

/// A unit of data that is stored in a
/// section. A `SectionItem` has an id
/// that can be used to resume from that
/// position in the section. Items may
/// be truncated e.g. if there was power
/// loss.
#[derive(Debug)]
pub(crate) struct SectionItem {
    pub id: u32,
    pub data: Vec<u8>,
    pub known_eof: bool,
    pub truncated: bool,
}

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
const MAX_FILE_SIZE: u32 = FAIL_FILE_SIZE - (3 * MAX_ITEM_SIZE);

const MAX_ITEM_SIZE: u32 = 134_217_728;

#[derive(Debug)]
pub(crate) struct SectionReader {
    path_buf: PathBuf,
    max_file_size: u32,
    max_item_size: u32,
    read_chunk_size: u32,
}

impl SectionReader {
    pub(crate) fn new(
        path: &PathBuf,
        max_file_size: u32,
        max_item_size: u32,
        read_chunk_size: u32,
    ) -> SectionReader {
        let path_buf = PathBuf::from(path);
        let max_file_size = cmp::min(MAX_FILE_SIZE, max_file_size);
        let max_item_size = cmp::min(MAX_ITEM_SIZE, max_item_size);

        SectionReader {
            path_buf,
            max_file_size,
            max_item_size,
            read_chunk_size,
        }
    }

    pub(crate) fn stream_with_truncated(
        &self,
        id: Option<u32>,
    ) -> io::Result<impl Iterator<Item = io::Result<SectionItem>>> {
        let mut file = self.open_file()?;

        // @FIXME should have a better error message so the user knows what's happening
        let mut always_fail = false;

        let mut position = if let Some(requested_id) = id {
            file.seek(SeekFrom::Start(requested_id as u64))?;
            requested_id
        } else {
            0
        };

        let buf_reader = BufReader::with_capacity(self.read_chunk_size as usize, file);

        let iterator = SplitWithCarry {
            buf: buf_reader,
            carry: None,
        };

        let max_file_size = self.max_file_size;
        let max_item_size = self.max_item_size;

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

                        let result = if id > max_item_size as usize {
                            always_fail = true;

                            Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("item at position {} exceeds the maximum length", position),
                            ))
                        } else {
                            position += 1 + id as u32;
                            let known_eof = position >= max_file_size;
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

    fn parse_item(id: u32, mut data: Vec<u8>, known_eof: bool) -> io::Result<SectionItem> {
        let len = data.len();

        let mut escaped = false;
        let mut i = 0;
        let mut t = 0;

        let truncated = len > 1 && data[len - 1] == MARKER_FAIL;

        if !truncated {
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

            data.truncate(len - t);
        }

        Ok(SectionItem {
            id,
            data,
            known_eof,
            truncated,
        })
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

/// A section is used to store items on disk and retrieve them.
///
/// Since a section can be become full, it is recommended to use
/// the higher level interfaces that manage the creation of
/// new sections for you, notably a Queue.
#[derive(Debug)]
pub(crate) struct SectionWriter {
    buffer: BufWriter<File>,
    item_buffer: [u8; 2],
    last_id: Option<u32>,
    position: u32,
    max_file_size: u32,
    max_item_size: u32,
    read_chunk_size: u32,
    write_chunk_size: u32,
}

impl SectionWriter {
    pub(crate) fn new(
        path: &PathBuf,
        max_file_size: u32,
        max_item_size: u32,
        read_chunk_size: u32,
        write_chunk_size: u32,
    ) -> io::Result<SectionWriter> {
        let max_file_size = cmp::min(MAX_FILE_SIZE, max_file_size);
        let max_item_size = cmp::min(MAX_ITEM_SIZE, max_item_size);

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

        let last_id = last_id(&mut file, position as u32, read_chunk_size)?;

        let buffer = BufWriter::with_capacity(write_chunk_size as usize, file);

        // Upto 2 bytes are needed for each step -- the relevant data and possibly
        // an escape.
        let item_buffer: [u8; 2] = [0; 2];

        Ok(SectionWriter {
            buffer,
            item_buffer,
            last_id,
            position,
            max_file_size,
            max_item_size,
            read_chunk_size,
            write_chunk_size,
        })
    }

    pub(crate) fn append(&mut self, data: &[u8]) -> io::Result<()> {
        if data.len() > self.max_item_size as usize {
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

    pub(crate) fn is_empty(&self) -> bool {
        self.last_id == None
    }

    pub(crate) fn is_full(&self) -> bool {
        self.position >= self.max_file_size
    }

    pub(crate) fn last_id(&mut self) -> Option<u32> {
        self.last_id
    }

    pub(crate) fn sync(&mut self) -> io::Result<()> {
        self.buffer.flush()
    }
}

/// Given an open file and its total length, extract the last id
/// that was written. Note that this by design only works with
/// 32bit unsigned integers in length, so the caller must validate
/// this before hand.
fn last_id(file: &mut File, length: u32, read_chunk_size: u32) -> io::Result<Option<u32>> {
    let mut buf = vec![0u8; read_chunk_size as usize];
    let mut total = 0;
    let mut items = 0;

    while total < length {
        let pos = length - total;

        let starting_at = if pos < read_chunk_size {
            0
        } else {
            pos - read_chunk_size
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
