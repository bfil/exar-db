use std::fmt::{Display, Formatter, Result as DisplayResult};

pub trait Validation where Self: Sized {
    fn validate(self) -> Result<Self, ValidationError>;
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

#[cfg(test)]
mod tests {
    use super::super::*;

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct Test {
        pub value: String
    }

    impl Validation for Test {
        fn validate(self) -> Result<Self, ValidationError> {
            if self.value == "invalid" {
                Err(ValidationError::new("invalid value"))
            } else {
                Ok(self)
            }
        }
    }

    #[test]
    fn test_validation() {
        let valid_test = Test { value: "valid".to_owned() };
        assert_eq!(valid_test.clone().validate(), Ok(valid_test));

        let invalid_test = Test { value: "invalid".to_owned() };
        assert_eq!(invalid_test.clone().validate(), Err(ValidationError::new("invalid value")));

        assert_eq!(format!("{}", ValidationError::new("invalid value")), "invalid value".to_owned());
    }
}
