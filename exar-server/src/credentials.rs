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
