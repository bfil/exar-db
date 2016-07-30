#![macro_use]

use std::fmt::{Debug, Display, Formatter, Result as DisplayResult};
use std::str::{FromStr, SplitN};

/// Generates a tab separated string from a list of string slices
///
/// # Examples
/// ```
/// #[macro_use]
/// extern crate exar;
///
/// # fn main() {
/// let tab_separated_value = tab_separated!("hello", "world");
/// # }
/// ```
#[macro_export]
macro_rules! tab_separated {
    ($($x:expr),*) => ({
        let vec: Vec<String> = vec![$($x.to_string()),*];
        vec.join("\t")
    })
}

/// A trait for serializing a type to a tab-separated string.
pub trait ToTabSeparatedString {
    /// Returns a tab-separated string from the value.
    fn to_tab_separated_string(&self) -> String;
}

/// A trait for deserializing a type from a tab-separated string slice.
pub trait FromTabSeparatedStr {
    /// Returns an instance of `Self` from a tab-separated string slice
    /// or a `ParseError` if a failure occurs while parsing the string.
    fn from_tab_separated_str(s: &str) -> Result<Self, ParseError> where Self: Sized;
}

/// A list specifying categories of parse error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The parsing failed because of the given reason.
    ParseError(String),
    /// The parsing failed because of a missing field at the given position.
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

/// A parser for tab-separated strings
///
/// # Examples
/// ```
/// #[macro_use]
/// extern crate exar;
///
/// # fn main() {
/// use exar::*;
///
/// let tab_separated_value = tab_separated!("hello", "world");
/// let mut parser = TabSeparatedParser::new(2, &tab_separated_value);
///
/// let hello: String = parser.parse_next().unwrap();
/// let world: String = parser.parse_next().unwrap();
/// # }
/// ```
pub struct TabSeparatedParser<'a> {
    index: usize,
    parts: SplitN<'a, &'a str>
}

impl<'a> TabSeparatedParser<'a> {
    /// Creates a new parser that splits a string up to `n` parts.
    pub fn new(n: usize, s: &'a str) -> TabSeparatedParser<'a> {
        TabSeparatedParser {
            index: 0,
            parts: s.splitn(n, "\t")
        }
    }

    /// Parses the next string slice into the given type `T` and returns it,
    /// or returns a `ParseError` if a failure occurs while parsing the value.
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

        assert_eq!(parser.parse_next::<u8>(), Err(ParseError::ParseError("invalid digit found in string".to_owned())));
    }

    #[test]
    fn test_missing_field_error() {
        let tab_separated_value = tab_separated!("hello", "world");
        let mut parser = TabSeparatedParser::new(2, &tab_separated_value);

        let hello: String = parser.parse_next().expect("Unable to parse value");
        let world: String = parser.parse_next().expect("Unable to parse value");

        assert_eq!(hello, "hello".to_owned());
        assert_eq!(world, "world".to_owned());

        assert_eq!(parser.parse_next::<String>(), Err(ParseError::MissingField(2)));
    }
}
