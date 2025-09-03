use super::memtable::MemTable;
use std::{
    fs::{File, OpenOptions, read_dir, remove_file},
    io::{self, BufReader, BufWriter, Read, Write},
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

pub struct WALEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

pub struct WALIterator {
    reader: BufReader<File>,
}

impl WALIterator {
    pub fn new(path: PathBuf) -> io::Result<WALIterator> {
        let file = OpenOptions::new().read(true).open(path)?;
        let reader = BufReader::new(file);
        Ok(WALIterator { reader })
    }
}

impl Iterator for WALIterator {
    type Item = WALEntry;

    fn next(&mut self) -> Option<WALEntry> {
        let mut len_buffer = [0; 8];
        if self.reader.read_exact(&mut len_buffer).is_err() {
            return None;
        }
        let key_len = usize::from_le_bytes(len_buffer);
        let mut bool_buffer = [0; 1];
        if self.reader.read_exact(&mut bool_buffer).is_err() {
            return None;
        }
        let deleted = bool_buffer[0] != 0;
        let mut key = vec![0; key_len];
        let mut value = None;
        if deleted {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
        } else {
            if self.reader.read_exact(&mut key).is_err() {
                return None;
            }
            let value_len = usize::from_le_bytes(len_buffer);
            if self.reader.read_exact(&mut len_buffer).is_err() {
                return None;
            }
            let mut value_buf = vec![0; value_len];
            if self.reader.read_exact(&mut value_buf).is_err() {
                return None;
            }
            value = Some(value_buf);
        }
        let mut timestamp_buffer = [0; 16];
        if self.reader.read_exact(&mut timestamp_buffer).is_err() {
            return None;
        }
        let timestamp = u128::from_le_bytes(timestamp_buffer);
        Some(WALEntry {
            key,
            value,
            timestamp,
            deleted: deleted,
        })
    }
}

pub struct WAL {
    path: PathBuf,
    file: BufWriter<File>,
}

impl WAL {
    pub fn new(dir: &Path) -> io::Result<WAL> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let path = Path::new(dir).join(timestamp.to_string() + ".wal");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let file = BufWriter::new(file);
        Ok(WAL { path, file })
    }

    pub fn from_path(path: &Path) -> io::Result<WAL> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let file = BufWriter::new(file);
        Ok(WAL {
            path: path.to_owned(),
            file,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&(key.len() as u64).to_le_bytes())?;
        self.file.write_all(&(false as u8).to_le_bytes())?;
        self.file.write_all(&value.len().to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(value)?;
        self.file.write_all(&timestamp.to_le_bytes())?;
        self.file.flush()?;
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) -> io::Result<()> {
        self.file.write_all(&(key.len() as u64).to_le_bytes())?;
        self.file.write_all(&(true as u8).to_le_bytes())?;
        self.file.write_all(key)?;
        self.file.write_all(&timestamp.to_le_bytes())?;
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl IntoIterator for WAL {
    type Item = WALEntry;
    type IntoIter = WALIterator;

    fn into_iter(self) -> Self::IntoIter {
        self.file.get_ref().sync_all().unwrap();
        WALIterator::new(self.path).unwrap()
    }
}

pub fn files_with_ext(dir: &Path, ext: &str) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for file in read_dir(dir).unwrap() {
        let path = file.unwrap().path();
        if path.extension().unwrap() == ext {
            files.push(path);
        }
    }

    files
}

pub fn load_from_dir(dir: &Path) -> io::Result<(WAL, MemTable)> {
    let mut wal_files = files_with_ext(dir, "wal");
    wal_files.sort();

    let mut new_mem_table = MemTable::new(10, 0.5);
    let mut new_wal = WAL::new(dir)?;
    for wal_file in wal_files.iter() {
        if let Ok(wal) = WAL::from_path(wal_file) {
            for entry in wal.into_iter() {
                if entry.deleted {
                    new_mem_table.delete(entry.key.as_slice(), entry.timestamp);
                    new_wal.delete(entry.key.as_slice(), entry.timestamp)?;
                } else {
                    new_mem_table.set(
                        entry.key.as_slice(),
                        Some(entry.value.as_ref().unwrap().as_slice()),
                        entry.timestamp,
                    );
                    new_wal.set(
                        entry.key.as_slice(),
                        entry.value.unwrap().as_slice(),
                        entry.timestamp,
                    )?;
                }
            }
        }
    }
    new_wal.flush().unwrap();
    wal_files.into_iter().for_each(|f| remove_file(f).unwrap());

    Ok((new_wal, new_mem_table))
}
