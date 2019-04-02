use std::io::prelude::*;
use std::io::{BufWriter, LineWriter, Result};

/// A trait for writing a line into a stream.
pub trait WriteLine {
    /// Writes a string slice into this writer by appending a new line at the end of it,
    /// returning whether the write succeeded.
    fn write_line(&mut self, line: &str) -> Result<usize>;
}

impl<T: Write> WriteLine for BufWriter<T> {
    fn write_line(&mut self, line: &str) -> Result<usize> {
        self.write(format!("{}\n", line).as_bytes())
    }
}

impl<T: Write> WriteLine for LineWriter<T> {
    fn write_line(&mut self, line: &str) -> Result<usize> {
        self.write(format!("{}\n", line).as_bytes())
    }
}

/// An interval.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Interval<T> {
    /// The start of the interval.
    pub start: T,
    /// The end of the interval.
    pub end: T
}

impl<T> Interval<T> {
    pub fn new(start: T, end: T) -> Interval<T> {
        Interval { start, end }
    }
}

/// A trait for merging a type.
pub trait Merge: Sized {
    fn merge(&mut self);
    fn merged(mut self) -> Self {
        self.merge();
        self
    }
}

impl Merge for Vec<Interval<u64>> {
    fn merge(&mut self) {
        if !self.is_empty() {
            self.sort_by(|a, b| a.start.cmp(&b.start));
            let mut merged_intervals = vec![ self[0].clone() ];
            for interval in self.iter().skip(1) {
                let last_pos = merged_intervals.len() - 1;
                if merged_intervals[last_pos].end < interval.start {
                    merged_intervals.push(interval.clone());
                } else if merged_intervals[last_pos].end >= interval.start &&
                          merged_intervals[last_pos].end <= interval.end {
                    merged_intervals[last_pos].end = interval.end;
                }
            }
            *self = merged_intervals;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::fs::*;
    use std::io::{BufRead, BufReader, BufWriter, Seek, SeekFrom};

    #[test]
    fn test_write_line() {
        let file = OpenOptions::new().read(true).write(true).create(true)
                                     .open("buf-writer.log").expect("Unable to create file");

        let mut buf_writer = BufWriter::new(file);
        assert!(buf_writer.write_line("line 1").is_ok());
        assert!(buf_writer.write_line("line 2").is_ok());

        let mut file = buf_writer.into_inner().expect("Unable to extract inner context");
        file.seek(SeekFrom::Start(0)).expect("Unable to seek to start");
        let mut lines = BufReader::new(file).lines();

        let line = lines.next().expect("Unable to read next line")
                               .expect("Unable to read next line");

        assert_eq!(line, "line 1".to_owned());

        let line = lines.next().expect("Unable to read next line")
                               .expect("Unable to read next line");

        assert_eq!(line, "line 2".to_owned());

        assert!(remove_file("buf-writer.log").is_ok());
    }

    #[test]
    fn test_intervals_merging() {
        let intervals = vec![];
        assert_eq!(intervals.merged(), vec![]);

        let intervals = vec![
            Interval::new(0, 10),
            Interval::new(30, 50),
            Interval::new(40, 70)
        ];
        assert_eq!(intervals.merged(), vec![Interval::new(0, 10), Interval::new(30, 70)]);
    }
}
