#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Credentials  {
    pub username: Option<String>,
    pub password: Option<String>
}

impl Credentials {
    pub fn new(username: &str, password: &str) -> Credentials {
        Credentials {
            username: Some(username.to_owned()),
            password: Some(password.to_owned())
        }
    }

    pub fn empty() -> Credentials {
        Credentials {
            username: None,
            password: None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constructors() {
        let credentials = Credentials::new("username", "password");
        assert_eq!(credentials.username, Some("username".to_owned()));
        assert_eq!(credentials.password, Some("password".to_owned()));

        let credentials = Credentials::empty();
        assert_eq!(credentials.username, None);
        assert_eq!(credentials.password, None);
    }
}
