use std::io::prelude::*;
use std::io::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BufWriter<T: Write> {
    inner: T
}

impl<T: Write> BufWriter<T> {
    pub fn new(stream: T) -> BufWriter<T> {
        BufWriter {
            inner: stream
        }
    }

    pub fn write_line(&mut self, line: &str) -> Result<()> {
        self.inner.write(format!("{}\n", line).as_bytes()).and_then(|_| {
            self.inner.flush()
        })
    }

    pub fn get_ref(&self) -> &T { &self.inner }

    pub fn get_mut(&mut self) -> &mut T { &mut self.inner }

    pub fn into_inner(self) -> T { self.inner }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::fs::*;
    use std::io::{BufRead, BufReader, Write, Seek, SeekFrom};

    #[test]
    fn test_write_line() {
        let file = OpenOptions::new().read(true).write(true).create(true)
                                     .open("buf-writer.log").expect("Unable to create file");

        let mut buf_writer = BufWriter::new(file);
        assert!(buf_writer.write_line("line 1").is_ok());
        assert!(buf_writer.write_line("line 2").is_ok());

        let mut file = buf_writer.into_inner();
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
}
