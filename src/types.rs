use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct WikiChange<'a> {
    pub user: Option<&'a str>,
    pub bot: Option<bool>,
    pub server_name: Option<&'a str>,
}