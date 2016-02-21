use std::fmt::Debug;
use std::str::{FromStr, SplitN};

pub struct TabSeparatedParser<'a> {
    index: usize,
    parts: SplitN<'a, &'a str>
}

impl<'a> TabSeparatedParser<'a> {
    pub fn new(n: usize, s: &'a str) -> TabSeparatedParser<'a> {
        TabSeparatedParser {
            index: 0,
            parts: s.splitn(n, "\t")
        }
    }

    pub fn parse_next<T>(&mut self) -> Result<T, ParseError> where T: FromStr, <T as FromStr>::Err: Debug {
        match self.parts.next().map(|x| x.parse())  {
            Some(Ok(value)) => {
                self.index += 1;
                Ok(value)
            },
            Some(Err(err)) => Err(ParseError::ParseError(format!("{:?}", err))),
            None => Err(ParseError::MissingField(self.index))
        }
    }
}

#[derive(Clone, Debug)]
pub enum ParseError {
    ParseError(String),
    MissingField(usize)
}
