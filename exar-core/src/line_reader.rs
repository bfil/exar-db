use std::io::{BufRead, Error, Read, Seek, SeekFrom};
use std::collections::BTreeMap;

pub struct LinesIndex {
    index: BTreeMap<u64, u64>,
    granularity: u64
}

impl LinesIndex {
    pub fn new(granularity: u64) -> LinesIndex {
        LinesIndex {
            index: BTreeMap::new(),
            granularity: granularity
        }
    }

    pub fn last_indexed_pos(&self) -> u64 {
        self.index.keys().map(|&x| x).max().unwrap_or(0)
    }

    pub fn get(&self, pos: &u64) -> u64 {
        self.index.get(pos).map(|&x| x).unwrap_or(0)
    }

    pub fn insert(&mut self, pos: u64, bytes_len: u64) -> Option<u64> {
        self.index.insert(pos as u64 + 2, bytes_len)
    }

    fn update<T: BufRead + Seek>(&mut self, mut reader: &mut T) -> Result<u64, Error> {
        let mut last_pos = self.last_indexed_pos();
        let mut bytes_len = self.get(&last_pos);
        if bytes_len > 0 {
            try!(reader.seek(SeekFrom::Start(bytes_len)));
            reader.lines().next();
        }
        for (pos, line) in reader.lines().enumerate() {
            match line {
                Ok(line) => {
                    bytes_len += line.as_bytes().len() as u64 + 1;
                    if (pos as u64 + 2) % self.granularity == 0 {
                        self.index.insert(pos as u64 + 2, bytes_len);
                    }
                    last_pos += 1;
                },
                Err(err) => return Err(err)
            }
        }
        Ok(last_pos)
    }

    pub fn clear(&mut self) {
        self.index.clear();
    }
}

pub struct IndexedLineReader<T> {
    index: LinesIndex,
    pos: u64,
    last_pos: u64,
    reader: T
}

impl<T: BufRead + Seek> IndexedLineReader<T> {
    pub fn new(reader: T, index_granularity: u64) -> IndexedLineReader<T> {
        IndexedLineReader {
            index: LinesIndex::new(index_granularity),
            pos: 0,
            last_pos: 0,
            reader: reader
        }
    }

    pub fn get_index(&self) -> &LinesIndex {
        &self.index
    }

    pub fn load_index(&mut self, index: LinesIndex) {
        self.index = index;
    }

    pub fn update_index(&mut self) -> Result<u64, Error> {
        self.index.update(&mut self.reader).and_then(|last_pos| {
            self.last_pos = last_pos;
            Ok(last_pos)
        })
    }

    pub fn clear_index(&mut self) {
        self.index.clear()
    }

    fn seek_to_index(&mut self, indexed_pos: u64) -> Result<u64, Error> {
        self.pos = indexed_pos;
        let bytes_len = self.index.get(&indexed_pos);
        self.reader.seek(SeekFrom::Start(bytes_len))
    }

    fn seek_to_closest_index(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        match pos {
            SeekFrom::Start(pos) => {
                let extra_lines = pos % self.index.granularity;
                let closest_index = pos - extra_lines;
                self.seek_to_index(closest_index)
            },
            SeekFrom::Current(pos) => {
                let extra_lines = pos as u64 % self.index.granularity;
                let extra_lines_from_current_pos = self.pos % self.index.granularity;
                let previous_closest_index = self.pos - extra_lines_from_current_pos;
                let closest_index = previous_closest_index + pos as u64 - extra_lines;
                self.seek_to_index(closest_index)
            },
            SeekFrom::End(pos) => {
                let pos = self.last_pos - pos.abs() as u64;
                self.seek_to_closest_index(SeekFrom::Start(pos))
            }
        }
    }

    fn seek_forward(&mut self, lines: u64) -> Result<u64, Error> {
        let mut lines_left = lines;
        let mut extra_bytes_len: u64 = 0;
        for line in (&mut self.reader).lines() {
            match line {
                Ok(line) => {
                    lines_left -= 1;
                    self.pos += 1;
                    extra_bytes_len += line.as_bytes().len() as u64 + 1;
                    if lines_left == 0 { break }
                },
                Err(err) => return Err(err)
            }
        }
        Ok(extra_bytes_len)
    }
}

impl<T: Read> Read for IndexedLineReader<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Error> {
        self.reader.read(buf)
    }
}

impl<T: BufRead> BufRead for IndexedLineReader<T> {
    fn fill_buf(&mut self) -> Result<&[u8], Error> {
        self.reader.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt)
    }
}

impl<T: BufRead + Seek> Seek for IndexedLineReader<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64, Error> {
        self.update_index().and_then(|_| {
            match pos {
                SeekFrom::Start(pos) => {
                    let extra_lines = pos as u64 % self.index.granularity;
                    self.seek_to_closest_index(SeekFrom::Start(pos)).and_then(|new_pos| {
                        if extra_lines > 0 {
                            self.seek(SeekFrom::Current(extra_lines as i64))
                        } else {
                            Ok(new_pos)
                        }
                    })
                },
                SeekFrom::Current(pos) => {
                    if pos >= 0 {
                        let extra_lines = pos as u64 % self.index.granularity;
                        let extra_lines_from_current_pos = self.pos % self.index.granularity;
                        self.seek_to_closest_index(SeekFrom::Current(pos)).and_then(|new_pos| {
                            if extra_lines + extra_lines_from_current_pos > 0 {
                                self.seek_forward(extra_lines + extra_lines_from_current_pos)
                            } else {
                                Ok(new_pos)
                            }
                        })
                    } else {
                        let pos = self.pos - pos.abs() as u64;
                        self.seek(SeekFrom::Start(pos))
                    }
                },
                SeekFrom::End(pos) => {
                    let pos = self.last_pos - pos.abs() as u64;
                    self.seek(SeekFrom::Start(pos))
                }
            }
        })
    }
}
