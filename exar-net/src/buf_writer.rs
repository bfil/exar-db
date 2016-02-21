use std::io::prelude::*;
use std::io::Result;

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
