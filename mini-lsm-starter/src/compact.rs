#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use core::task;
use std::borrow::Borrow;
use std::char::ToUppercase;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Ok, Result};
use arc_swap::Guard;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();
        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }
            iter.next()?;

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // lock dropped here
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(id).unwrap().clone(),
                    )?));
                }
                let mut l1_tables = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_tables.push(snapshot.sstables[id].clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_tables)?,
                )?;
                return self.compact_generate_sst_from_iter(iter, true);
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
                None => {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(id).unwrap().clone(),
                        )?));
                    }
                    let upper_iter = MergeIterator::create(upper_iters);
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            },
            CompactionTask::Leveled(task) => {
                todo!()
            }
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut tier_iters = Vec::with_capacity(tiers.len());
                for (_, tier_sst) in tiers.iter() {
                    let mut tables = Vec::with_capacity(tier_sst.len());
                    for sst_id in tier_sst.iter() {
                        let table = snapshot.sstables.get(sst_id).unwrap().clone();
                        tables.push(table);
                    }
                    let tier_iter = SstConcatIterator::create_and_seek_to_first(tables)?;
                    tier_iters.push(Box::new(tier_iter));
                }
                let iter = MergeIterator::create(tier_iters);
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();
        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };

        let sstables = self.compact(&compaction_task)?;
        let mut ids = Vec::with_capacity(sstables.len());
        {
            let state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();
            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(result.is_none());
            }
            assert_eq!(l1_sstables, state.levels[0].1);
            state.levels[0].1.clone_from(&ids);
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            // to filter the compact un-relative sst
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            assert!(l0_sstables_map.is_empty());
            *self.state.write() = Arc::new(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(compaction_task, ids.clone()),
            )?;
        };
        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }
        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let Some(task) = task else {
            return Ok(());
        };
        // println!("running compaction task: {:?}", task);
        let sstables = self.compact(&task)?;
        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();
        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = Vec::new();
            for file_to_add in sstables {
                new_sst_ids.push(file_to_add.sst_id());
                let result = snapshot.sstables.insert(file_to_add.sst_id(), file_to_add);
                assert!(result.is_none());
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_to_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_to_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_to_remove);
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            self.sync_dir()?;
            self.manifest.as_ref().unwrap().add_record(
                &state_lock,
                ManifestRecord::Compaction(task, new_sst_ids.clone()),
            )?;
            ssts_to_remove
        };
        // println!(
        //     "compaction finished: {} files removed, {} files added, output={:?}",
        //     ssts_to_remove.len(),
        //     output.len(),
        //     output
        // );
        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        self.sync_dir()?;
        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let mem_num = {
            let guard = self.state.read();
            guard.imm_memtables.len() + 1
        };
        if mem_num > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
