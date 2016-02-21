use std::fs::*;
use std::io::{BufRead, BufReader, Result, Write};

#[derive(Clone)]
pub struct Log {
    path: String,
    name: String
}

impl Log {
    pub fn new(path: &str, name: &str) -> Log {
        Log {
            path: path.to_owned(),
            name: name.to_owned()
        }
    }

    pub fn open_reader(&self) -> Result<File> {
        OpenOptions::new()
            .read(true)
            .open(self.get_path())
    }

    pub fn open_writer(&self) -> Result<File> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(self.get_path())
    }

    pub fn remove(&self) -> Result<()> {
        remove_file(self.get_path())
    }

    pub fn count_lines(&self) -> Result<usize> {
        match self.open_reader() {
            Ok(file) => Ok(BufReader::new(file).lines().count()),
            Err(err) => Err(err)
        }
    }

    pub fn get_path(&self) -> String {
        if self.path.is_empty() {
            format!("{}.log", self.name)
        } else {
            format!("{}/{}.log", self.path, self.name)
        }
    }
}
