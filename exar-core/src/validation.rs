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
