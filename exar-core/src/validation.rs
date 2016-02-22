use std::fmt::{Display, Formatter, Result as DisplayResult};

pub trait Validation<T: Sized> {
    fn validate(self) -> Result<T, ValidationError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationError {
    pub description: String
}

impl ValidationError {
    pub fn new(description: &str) -> ValidationError {
        ValidationError {
            description: description.to_owned()
        }
    }
}

impl Display for ValidationError {
    fn fmt(&self, f: &mut Formatter) -> DisplayResult {
        write!(f, "{}", self.description)
    }
}
