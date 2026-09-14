//! A config secret: reads from plain TOML, never prints in clear.

use std::fmt;

use serde::{Deserialize, Deserializer};

/// Wraps a value (e.g. a password) so it stays out of `Debug`/log output.
#[derive(Clone)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Borrow the inner value.
    pub fn expose(&self) -> &T {
        &self.0
    }
}

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret([REDACTED])")
    }
}

impl<'de, T> Deserialize<'de> for Secret<T>
where
    T: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Secret)
    }
}
