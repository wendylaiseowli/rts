use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct WikiChange<'a> {
    #[serde(borrow)]
    pub meta: Option<ChangeMeta<'a>>,
    pub user: Option<&'a str>,
    pub bot: Option<bool>,
    pub server_name: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
pub struct ChangeMeta<'a> {
    pub id: Option<&'a str>,
}