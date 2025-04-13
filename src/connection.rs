use anyhow::Error;
use odbc_api::{Environment, Connection};
use once_cell::sync::Lazy;

#[derive(Debug, Clone, Default)]
pub struct ConnectOpts {
    /// Connection string used to connect to the database.
    pub connection_string: Option<String>,
    /// Maximum number of retries after a connection was lost.
    pub max_connect_retries: u32,
    /// Time in seconds to wait between connect retries.
    pub connect_retry_interval: u64,
}

// ? Static global environment to avoid borrow lifetime issues
static ENV: Lazy<Environment> = Lazy::new(|| {
    Environment::new().expect("Failed to initialize ODBC environment")
});

pub fn open_connection(opts: &ConnectOpts) -> Result<Connection<'static>, Error> {
    let conn_str = opts.connection_string.as_ref()
        .ok_or_else(|| Error::msg("Connection string is missing"))?;
    let conn = ENV.connect_with_connection_string(conn_str, Default::default())?;
    Ok(conn)
}
