//! State framing codec (PLAN §13): `RocksDB` value = `u64 LE version ++ json`;
//! Cassandra keeps `version` as its own column + plain JSON.

/// Frame a stored state for `RocksDB`: 8-byte LE version header followed by the JSON blob.
#[must_use]
pub fn frame(version: i64, json: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + json.len());
    out.extend_from_slice(&version.to_le_bytes());
    out.extend_from_slice(json);
    out
}

/// Unframe a `RocksDB` value into `(version, json)`; `None` if the header is truncated.
#[must_use]
pub fn unframe(bytes: &[u8]) -> Option<(i64, &[u8])> {
    let (head, rest) = bytes.split_at_checked(8)?;
    let version = i64::from_le_bytes(head.try_into().ok()?);
    Some((version, rest))
}
