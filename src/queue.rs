use section::{Decoded, Section, SectionReader};
use std::fs;
use std::fs::DirEntry;
use std::io;
use std::path::{Path, PathBuf};

const MAX_COMPONENT_VALUE: u16 = 1000;

const MAX_COMPONENT_ENCODED_VALUE: u32 = 2147483647;

#[derive(Debug, PartialEq)]
pub struct Component {
    one: u16,
    two: u16,
    three: u16,
    four: u16,
}

/// A Component represents a path to a file on disk. It is
/// split into four components that count from 0 to 999.
///
/// A component can always be represented as a 32bit integer,
/// and its maximum value is 1,999,999,999.
///
/// Example: (0, 1, 2, 3) maps to the file <base>/0/1/2/3
impl Component {
    fn decode(encoded: u32) -> io::Result<Component> {
        let v = MAX_COMPONENT_VALUE as u32;

        if encoded <= MAX_COMPONENT_ENCODED_VALUE {
            let one = encoded / (v * v * v);
            let two = (encoded % (v * v * v)) / (v * v);
            let three = (encoded % (v * v)) / v;
            let four = encoded % v;

            Ok(Component {
                one: one as u16,
                two: two as u16,
                three: three as u16,
                four: four as u16,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "encoded component exceeds maximum value",
            ))
        }
    }

    fn new() -> Component {
        Component {
            one: 0,
            two: 0,
            three: 0,
            four: 0,
        }
    }

    fn from(one: u16, two: u16, three: u16, four: u16) -> io::Result<Component> {
        if one <= 1 && two < MAX_COMPONENT_VALUE && three < MAX_COMPONENT_VALUE
            && four < MAX_COMPONENT_VALUE
        {
            Ok(Component {
                one,
                two,
                three,
                four,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::Other,
                "encoded component exceeds maximum value",
            ))
        }
    }

    fn encode(&self) -> u32 {
        let v = MAX_COMPONENT_VALUE as u32;

        self.one as u32 * v * v * v + self.two as u32 * v * v + self.three as u32 * v
            + self.four as u32
    }

    fn is_empty(&self) -> bool {
        self.one == 0 && self.two == 0 && self.three == 0 && self.four == 0
    }

    fn is_full(&self) -> bool {
        let m = MAX_COMPONENT_VALUE - 1;

        self.one == m && self.two == m && self.three == m && self.four == m
    }

    fn next(&self) -> Option<Component> {
        if self.four < MAX_COMPONENT_VALUE - 1 {
            Some(Component {
                one: self.one,
                two: self.two,
                three: self.three,
                four: self.four + 1,
            })
        } else if self.three < MAX_COMPONENT_VALUE - 1 {
            Some(Component {
                one: self.one,
                two: self.two,
                three: self.three + 1,
                four: 0,
            })
        } else if self.two < MAX_COMPONENT_VALUE - 1 {
            Some(Component {
                one: self.one,
                two: self.two + 1,
                three: 0,
                four: 0,
            })
        } else if self.one < MAX_COMPONENT_VALUE - 1 {
            Some(Component {
                one: self.one + 1,
                two: 0,
                three: 0,
                four: 0,
            })
        } else {
            None
        }
    }

    fn paths<P: AsRef<Path>>(&self, base: P) -> (PathBuf, PathBuf) {
        let parent = base.as_ref()
            .join(format!("d{}", self.one))
            .join(format!("d{}", self.two))
            .join(format!("d{}", self.three));

        let file = parent.join(format!("d{}", self.four));

        (parent, file)
    }
}

#[test]
fn test_component() {
    let component = Component::new();

    assert_eq!(component.encode(), 0);

    assert_eq!(Component::decode(0).unwrap(), component);

    let component = component.next().unwrap();

    assert_eq!(component.encode(), 1);

    assert_eq!(Component::decode(1).unwrap(), component);

    let mut component = Component::new();

    for n in 0..10000 {
        assert_eq!(component, Component::decode(n).unwrap());

        component = component.next().unwrap();
    }
}

pub struct Queue {
    component_section: Option<(Component, Section)>,
    path_buf: PathBuf,
}

impl Queue {
    pub fn new(path: &PathBuf) -> Queue {
        let path_buf = PathBuf::from(path);

        Queue {
            component_section: None,
            path_buf,
        }
    }

    fn with<A, F>(&mut self, f: F) -> io::Result<A>
    where
        F: Fn(&Component, &mut Section) -> io::Result<A>,
    {
        if self.component_section.is_none() {
            fs::create_dir_all(&self.path_buf)?;

            let (c0_path, c0) = depot_latest_init_dir(&self.path_buf)?;
            let (c1_path, c1) = depot_latest_init_dir(&c0_path)?;
            let (c2_path, c2) = depot_latest_init_dir(&c1_path)?;
            let (c3_path, c3) = depot_latest_init_file(&c2_path)?;

            self.component_section =
                Some((Component::from(c0, c1, c2, c3)?, Section::new(&c3_path)?));
        }

        match self.component_section {
            Some((ref component, ref mut section)) => f(component, section),

            None => {
                // this shouldn't be possible, given initialization above..
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "section not initialized; this is likely a bug",
                ))
            }
        }
    }

    pub fn append(&mut self, data: &[u8]) -> io::Result<()> {
        let advance_and_append = self.with(|ref _component, ref mut section| {
            if section.is_full() {
                Ok(true)
            } else {
                section.append(data)?;

                Ok(false)
            }
        })?;

        if advance_and_append {
            self.advance()?;

            self.with(|ref _component, ref mut section2| section2.append(data))
        } else {
            Ok(())
        }
    }

    pub fn is_empty(&mut self) -> io::Result<bool> {
        self.with(|ref component, ref mut section| Ok(component.is_empty() && section.is_empty()))
    }

    pub fn is_full(&mut self) -> io::Result<bool> {
        self.with(|ref component, ref mut section| Ok(component.is_full() && section.is_full()))
    }

    pub fn last_id(&mut self) -> io::Result<Option<u32>> {
        self.with(|ref _component, ref mut section| Ok(section.last_id()))
    }

    pub fn sync(&mut self) -> io::Result<()> {
        self.with(|ref _component, ref mut section| section.sync())
    }

    fn advance(&mut self) -> io::Result<()> {
        let path_buf = self.path_buf.clone();
        let next_component_section = self.with(|ref component, ref mut section| {
            section.sync()?;

            match component.next() {
                Some(c) => {
                    // @TODO move the base path directly into components
                    let (parent, path) = c.paths(&path_buf);

                    fs::create_dir_all(&parent)?;

                    let section = Section::new(&path)?;

                    Ok((c, section))
                }

                None => Err(io::Error::new(io::ErrorKind::Other, "queue is full")),
            }
        })?;

        self.component_section = Some(next_component_section);

        Ok(())
    }

    pub fn stream(
        &self,
        id: Option<u64>,
    ) -> io::Result<impl Iterator<Item = io::Result<QueueItem>>> {
        let (component, section_offset) = match id {
            Some(id) => offset_decode(id)?,
            None => (Component::new(), 0),
        };

        // @FIXME have the struct take a reference equal to our lifetime?
        Ok(QueueIterator::new(
            self.path_buf.clone(),
            component,
            section_offset,
        ))
    }

    /*
    pub fn stream(&self, id: Option<u32>) -> io::Result<impl Iterator<Item = io::Result<Item>>> {
    }

    pub fn stream_with_truncated(&self, id: Option<u32>) -> io::Result<impl Iterator<Item = io::Result<Decoded>>> {
    }*/
}

#[derive(Debug)]
pub struct QueueItem {
    pub id: u64,
    pub data: Vec<u8>,
}

struct QueueIterator {
    component: Component,
    known_eof: bool,
    path_buf: PathBuf,
    section: Option<Box<Iterator<Item = io::Result<Decoded>>>>,
    section_offset: u32,
}

impl QueueIterator {
    fn new(path_buf: PathBuf, component: Component, section_offset: u32) -> QueueIterator {
        QueueIterator {
            component,
            known_eof: false,
            path_buf,
            section: None,
            section_offset,
        }
    }
}

impl Iterator for QueueIterator {
    type Item = io::Result<QueueItem>;

    fn next(&mut self) -> Option<io::Result<QueueItem>> {
        // The last file we read indicated EOF, so we need
        // to advance sections or bail out if unable to.
        if self.known_eof {
            match self.component.next() {
                Some(c) => {
                    self.component = c;
                    self.known_eof = false;
                    self.section = None;
                    self.section_offset = 0;
                }

                None => {
                    return None;
                }
            }
        }

        // We haven't opened the next section yet, so attempt to.
        // If it doesn't exist, we do nothing. If it does, attempt to
        // open the file. If that fails, which should be rare, return
        // to the user as an error.
        if self.section.is_none() {
            let (_, section_path) = self.component.paths(&self.path_buf);

            if section_path.exists() {
                let reader = SectionReader::new(&section_path);

                match reader.stream_with_truncated(Some(self.section_offset)) {
                    Ok(iterator) => {
                        self.section = Some(Box::new(iterator));
                    }

                    Err(e) => {
                        return Some(Err(e));
                    }
                }
            }
        }

        match self.section {
            Some(ref mut s) => match s.next() {
                Some(Ok(Decoded::Item(i))) => {
                    let id = offset_encode(&self.component, i.id);

                    self.known_eof = i.known_eof;
                    self.section_offset = i.id;

                    Some(Ok(QueueItem { id, data: i.data }))
                }

                Some(Ok(Decoded::TruncatedItem(i))) => {
                    let id = offset_encode(&self.component, i.id);

                    self.known_eof = i.known_eof;
                    self.section_offset = i.id;

                    Some(Ok(QueueItem { id, data: i.data }))
                }

                Some(Err(e)) => Some(Err(e)),

                None => None,
            },

            None => None,
        }
    }
}

/// Extracts the number from a depot directory/file name.
fn depot_number(name: &str) -> Option<u16> {
    let len = name.len();
    if name.starts_with("d") && len > 1 {
        name[1..].parse().ok().and_then(|n| {
            // @FIXME use Option#filter when in stable
            if n <= MAX_COMPONENT_VALUE {
                Some(n)
            } else {
                None
            }
        })
    } else {
        None
    }
}

/// Finds the latest directory in the specified directory (not recursive). The
/// path should already exist and be a directory.
///
/// If a directory does not exist, it should be created.
///
/// @TODO should we ensure that it's a directory if it already exists?
fn depot_latest_init_dir<P: AsRef<Path>>(path: P) -> io::Result<(PathBuf, u16)> {
    match depot_latest(&path)? {
        Some((entry, n)) => Ok((entry.path(), n)),

        None => {
            let path = path.as_ref().join(format!("d0"));
            fs::create_dir(&path)?;
            Ok((path, 0))
        }
    }
}

/// Finds the latest file in the directory (not recursive). The path
/// should already exist and be a directory.
///
/// Unlike the directory variant, if a file does not exist,
/// its name is determined but it is not created. This is
/// because the underlying section will create the file when
/// it's opened.
///
/// @TODO should we ensure that it's NOT a directory if it already exists?
fn depot_latest_init_file<P: AsRef<Path>>(path: P) -> io::Result<(PathBuf, u16)> {
    match depot_latest(&path)? {
        Some((entry, n)) => Ok((entry.path(), n)),

        None => {
            let path = path.as_ref().join(format!("d0"));
            Ok((path, 0))
        }
    }
}

/// Finds the latest depot file or directory in a directory
fn depot_latest<P: AsRef<Path>>(path: P) -> io::Result<Option<(DirEntry, u16)>> {
    let paths = fs::read_dir(path)?;

    let mut max = None;

    for entry in paths {
        let entry = entry?;

        if let Some(n) = entry.file_name().to_str().and_then(depot_number) {
            match max {
                Some((_, en)) if en > n => (),
                _ => max = Some((entry, n)),
            }
        }
    }

    Ok(max)
}

fn offset_encode(component: &Component, section_offset: u32) -> u64 {
    let f = component.encode() as u64;
    let s = section_offset as u64;

    (f << 32) + s
}

fn offset_decode(offset: u64) -> io::Result<(Component, u32)> {
    let f = (offset >> 32) as u32;
    let s = (offset << 32 >> 32) as u32;
    let c = Component::decode(f)?;

    Ok((c, s))
}

#[test]
fn test_offset_encode_decode() {
    let test = |component: Component, section_offset: u32, expected: u64| {
        let encoded = offset_encode(&component, section_offset);
        let decoded = offset_decode(encoded).unwrap();

        assert_eq!(encoded, expected);
        assert_eq!((component, section_offset), decoded);
    };

    test(Component::new(), 0, 0);
    test(Component::new(), 1, 1);
    test(Component::from(0, 0, 0, 1).unwrap(), 0, 1 << 32);
    test(Component::from(0, 0, 0, 2).unwrap(), 1, (1 << 33) + 1);
    test(
        Component::from(1, 999, 999, 999).unwrap(),
        1,
        8589934587705032705,
    );
}

#[cfg(test)]
mod tests {
    extern crate tempdir;

    use queue::*;
    use std::path::PathBuf;
    use std::thread;
    use std::time;

    #[test]
    fn test_reader_writer_concurrent() {
        return;
        let tmp_dir = tempdir::TempDir::new("depot-tests").unwrap();
        let size = 10_000_000;

        let producer = {
            let tmp_path = tmp_dir.path().to_owned();

            thread::spawn(move || {
                let mut queue = Queue::new(&PathBuf::from(&tmp_path));

                for i in 0..size {
                    let message =
                        format!("the quick brown fox jumped over the lazy dog, -\n #{}", i);
                    let data = message.as_bytes();
                    queue.append(&data).unwrap();
                }

                queue.sync().unwrap();
            })
        };

        let consumer = {
            return;
            let tmp_path = tmp_dir.path().to_owned();

            thread::spawn(move || {
                let queue = Queue::new(&PathBuf::from(&tmp_path));
                let mut reader = queue.stream(None).unwrap();

                for _ in 0..size {
                    loop {
                        if let Some(_) = reader.next() {
                            break;
                        } else {
                            thread::sleep(time::Duration::from_millis(10));
                        }
                    }
                }
            })
        };

        let pr = producer.join();
        let cr = consumer.join();

        pr.unwrap();
        cr.unwrap();
    }

    #[test]
    fn test_reader_writer_sequential() {
        let tmp_dir = tempdir::TempDir::new("depot-tests").unwrap();
        let size = 1_000_000;

        {
            let tmp_path = tmp_dir.path().to_owned();

            let mut queue = Queue::new(&PathBuf::from(&tmp_path));

            for i in 0..size {
                let message = format!("the quick brown fox jumped over the lazy dog, -\n #{}", i);
                let data = message.as_bytes();
                queue.append(&data).unwrap();
            }

            queue.sync().unwrap();
        }

        {
            let tmp_path = tmp_dir.path().to_owned();

            let queue = Queue::new(&PathBuf::from(&tmp_path));
            let mut reader = queue.stream(None).unwrap();

            for _ in 0..size {
                loop {
                    if let Some(_) = reader.next() {
                        break;
                    } else {
                        thread::sleep(time::Duration::from_millis(10));
                    }
                }
            }
        }
    }
}
