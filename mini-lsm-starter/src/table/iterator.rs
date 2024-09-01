#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::{io::Read, sync::Arc};

use anyhow::{bail, Ok, Result};

use super::SsTable;
use crate::{
    block::{Block, BlockIterator},
    iterators::StorageIterator,
    key::KeySlice,
    lsm_storage::BlockCache,
};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    fn seek_to_first_inner(table: &Arc<SsTable>) -> Result<(usize, BlockIterator)> {
        Ok((
            0,
            BlockIterator::create_and_seek_to_first(table.read_block_cached(0)?),
        ))
    }
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let (idx, iter) = SsTableIterator::seek_to_first_inner(&table)?;
        Ok(Self {
            table,
            blk_iter: iter,
            blk_idx: idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let (idx, iter) = SsTableIterator::seek_to_first_inner(&self.table)?;
        self.blk_idx = idx;
        self.blk_iter = iter;
        Ok(())
    }

    fn seek_to_key_inner(table: &Arc<SsTable>, key: KeySlice) -> Result<(usize, BlockIterator)> {
        let mut idx = table.find_block_idx(key);
        let mut blk_iter =
            BlockIterator::create_and_seek_to_key(table.read_block_cached(idx)?, key);
        // Therefore, we should check if the iterator is invalid after the seek, and switch to the next block if necessary.
        if !blk_iter.is_valid() {
            idx += 1;
            if idx < table.num_of_blocks() {
                blk_iter = BlockIterator::create_and_seek_to_first(table.read_block_cached(idx)?);
            }
        }
        Ok((idx, blk_iter))
    }
    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let (idx, iter) = SsTableIterator::seek_to_key_inner(&table, key)?;
        Ok(Self {
            table,
            blk_iter: iter,
            blk_idx: idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        let (idx, iter) = SsTableIterator::seek_to_key_inner(&self.table, key)?;
        self.blk_idx = idx;
        self.blk_iter = iter;
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        // BlockIter out of bound, new block require
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.num_of_blocks() {
                self.blk_iter = BlockIterator::create_and_seek_to_first(
                    self.table.read_block_cached(self.blk_idx)?,
                );
            }
        }
        Ok(())
    }
}
