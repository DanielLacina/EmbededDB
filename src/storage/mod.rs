mod sstable;
mod wal;
mod hnsw;
mod memtable;
mod node;
use wal::WAL;
use memtable::MemTable;


pub struct Storage {
   wal: WAL,
   mem_table: MemTable
}



