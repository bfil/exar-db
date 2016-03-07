use std::io::prelude::*;
use std::io::{BufWriter, Result};

pub trait WriteLine {
    fn write_line(&mut self, line: &str) -> Result<usize>;
}

impl<T: Write> WriteLine for BufWriter<T> {
    fn write_line(&mut self, line: &str) -> Result<usize> {
        self.write(format!("{}\n", line).as_bytes()).and_then(|bytes_written| {
            self.flush().and_then(|_| {
                Ok(bytes_written)
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    use std::fs::*;
    use std::io::{BufRead, BufReader, BufWriter, Write, Seek, SeekFrom};

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
}
