use std::fmt::{Display, Formatter, Result as DisplayResult};

/// A trait for validating a type.
pub trait Validation where Self: Sized {
    /// Validates the type or returns a `ValidationError` if validation fails.
    fn validate(&self) -> Result<(), ValidationError>;
    /// Validates and returns `Self` or a `ValidationError` if validation fails.
    fn validated(self) -> Result<Self, ValidationError> {
        self.validate().and_then(|_| Ok(self))
    }
}

/// A validation error.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidationError {
    /// The validation error's description.
    pub description: String
}

impl ValidationError {
    /// Creates a `ValidationError` with the given description.
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
        fn validate(&self) -> Result<(), ValidationError> {
            if self.value == "invalid" {
                return Err(ValidationError::new("invalid value"));
            }
            Ok(())
        }
    }

    #[test]
    fn test_validation() {
        let valid_test = Test { value: "valid".to_owned() };
        assert_eq!(valid_test.clone().validate(), Ok(()));

        let valid_test = Test { value: "valid".to_owned() };
        assert_eq!(valid_test.clone().validated(), Ok(valid_test));

        let invalid_test = Test { value: "invalid".to_owned() };
        assert_eq!(invalid_test.clone().validate(), Err(ValidationError::new("invalid value")));

        assert_eq!(format!("{}", ValidationError::new("invalid value")), "invalid value".to_owned());
    }
}
