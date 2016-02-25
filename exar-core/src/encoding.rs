#![macro_use]

use std::fmt::{Debug, Display, Formatter, Result as DisplayResult};
use std::str::{FromStr, SplitN};

#[macro_export]
macro_rules! tab_separated {
    ($($x:expr),*) => ({
        let vec: Vec<String> = vec![$($x.to_string()),*];
        vec.join("\t")
    })
}

pub trait ToTabSeparatedString {
    fn to_tab_separated_string(&self) -> String;
}

pub trait FromTabSeparatedStr {
    fn from_tab_separated_str(s: &str) -> Result<Self, ParseError> where Self: Sized;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    ParseError(String),
    MissingField(usize)
}

impl Display for ParseError {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        match *self {
            ParseError::ParseError(ref description) => write!(f, "{}", description),
            ParseError::MissingField(index) => write!(f, "missing field at index {}", index)
        }
    }
}

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

    pub fn parse_next<T>(&mut self) -> Result<T, ParseError> where T: FromStr, <T as FromStr>::Err: Display + Debug {
        match self.parts.next().map(|x| x.parse())  {
            Some(Ok(value)) => {
                self.index += 1;
                Ok(value)
            },
            Some(Err(err)) => Err(ParseError::ParseError(format!("{}", err))),
            None => Err(ParseError::MissingField(self.index))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::*;

    #[test]
    fn test_tab_separated_macro() {
        let tab_separated_value = tab_separated!("hello", "world", "!");
        assert_eq!(tab_separated_value, "hello\tworld\t!");

        let tab_separated_value = tab_separated!(1, 2);
        assert_eq!(tab_separated_value, "1\t2");
    }

    #[test]
    fn test_tab_separated_parser() {
        let tab_separated_value = tab_separated!("hello", "world", "!");
        let mut parser = TabSeparatedParser::new(3, &tab_separated_value);

        let hello: String = parser.parse_next().expect("Unable to parse value");
        let world: String = parser.parse_next().expect("Unable to parse value");
        let exclamation_mark: String = parser.parse_next().expect("Unable to parse value");

        assert_eq!(hello, "hello".to_owned());
        assert_eq!(world, "world".to_owned());
        assert_eq!(exclamation_mark, "!".to_owned());

        let tab_separated_value = tab_separated!(1, 2);
        let mut parser = TabSeparatedParser::new(2, &tab_separated_value);

        let one: u8 = parser.parse_next().expect("Unable to parse value");
        let two: u8 = parser.parse_next().expect("Unable to parse value");

        assert_eq!(one, 1);
        assert_eq!(two, 2);
    }

    #[test]
    fn test_parse_error() {
        let tab_separated_value = tab_separated!("hello", "world");
        let mut parser = TabSeparatedParser::new(2, &tab_separated_value);

        let hello: Result<u8, ParseError> = parser.parse_next();
        let parse_error = hello.err().expect("Unable to extract error");

        assert_eq!(parse_error, ParseError::ParseError("invalid digit found in string".to_owned()));
    }

    #[test]
    fn test_missing_field_error() {
        let tab_separated_value = tab_separated!("hello", "world");
        let mut parser = TabSeparatedParser::new(3, &tab_separated_value);

        let hello: String = parser.parse_next().expect("Unable to parse value");
        let world: String = parser.parse_next().expect("Unable to parse value");
        let exclamation_mark: Result<String, ParseError> = parser.parse_next();
        let parse_error = exclamation_mark.err().expect("Unable to extract error");

        assert_eq!(hello, "hello".to_owned());
        assert_eq!(world, "world".to_owned());
        assert_eq!(parse_error, ParseError::MissingField(2));
    }
}
