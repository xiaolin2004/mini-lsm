#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::fs::{File, OpenOptions};

use std::io::{BufWriter, Read, Write};
use std::path::{self, Path};
use std::sync::Arc;

use anyhow::{Context, Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use rustyline::validate;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                OpenOptions::new()
                    .read(true)
                    .create_new(true)
                    .write(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let path = path.as_ref();
        let mut file = OpenOptions::new().read(true).append(true).open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let mut rbuf = buf.as_slice();
        while rbuf.has_remaining() {
            let key_len = rbuf.get_u32();
            let key = Bytes::copy_from_slice(&rbuf[..key_len as usize]);
            rbuf.advance(key_len as usize);
            let val_len = rbuf.get_u32();
            let value = Bytes::copy_from_slice(&rbuf[..val_len as usize]);
            rbuf.advance(val_len as usize);
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut file = self.file.lock();
        let mut buf = Vec::with_capacity(key.len() + value.len() + 2 * size_of::<u32>());
        buf.put_u32(key.len() as u32);
        buf.put_slice(key);
        buf.put_u32(value.len() as u32);
        buf.put_slice(value);
        file.write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
