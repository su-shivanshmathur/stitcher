//! `DynamoDB` backend — **stub** (PLAN §4): every operation returns a typed `Unsupported`
//! error (`unimplemented!` is forbidden by lints); never selected by default.

use std::collections::HashMap;

use super::{PartitionRef, RebalanceEvent, Store};
use crate::{
    config,
    errors::{StitcherError, StitcherResult},
    processor::Key,
};

/// Stub `DynamoDB` store — all operations error until the backend is implemented.
#[derive(Debug)]
pub struct DynamoStore;

impl DynamoStore {
    /// Construct the stub; runtime use yields `Unsupported` on every operation.
    #[must_use]
    pub fn new(_cfg: &config::StoreCfg) -> Self {
        Self
    }

    fn unsupported<T>() -> StitcherResult<T> {
        Err(error_stack::report!(StitcherError::Unsupported(
            "dynamodb backend not implemented yet (PLAN §4)"
        )))
    }
}

#[async_trait::async_trait]
impl Store for DynamoStore {
    async fn get_many(
        &self,
        _id_type: &str,
        _keys: &[Key],
    ) -> StitcherResult<HashMap<Key, (i64, Vec<u8>)>> {
        Self::unsupported()
    }

    async fn put(
        &self,
        _id_type: &str,
        _key: &Key,
        _version: i64,
        _blob: &[u8],
        _part: &PartitionRef,
    ) -> StitcherResult<()> {
        Self::unsupported()
    }

    async fn on_rebalance(&self, _ev: &RebalanceEvent) -> StitcherResult<()> {
        Ok(())
    }

    async fn cleanup(&self) -> StitcherResult<()> {
        Ok(())
    }
}
