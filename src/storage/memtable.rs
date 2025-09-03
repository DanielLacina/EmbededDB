use std::cell::RefCell;
use std::rc::Rc;

type MemTableElementRef = Rc<RefCell<MemTableElement>>;

struct MemTableElement {
    entry: MemTableEntry,
    next: Vec<MemTableElementRef>,
}

impl MemTableElement {
    pub fn new(entry: MemTableEntry) -> Self {
        Self {
            entry,
            next: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemTableEntry {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub timestamp: u128,
    pub deleted: bool,
}

impl PartialEq for MemTableEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value && self.deleted == other.deleted
    }
}

impl MemTableEntry {
    fn new(key: Vec<u8>, value: Option<Vec<u8>>, timestamp: u128) -> Self {
        Self {
            key,
            value,
            deleted: false,
            timestamp,
        }
    }
}

pub struct MemTable {
    max_level: usize,
    p: f64,
    level: usize,
    size: usize,
    head: MemTableElementRef,
}

impl MemTable {
    pub fn new(max_level: usize, p: f64) -> Self {
        MemTable {
            max_level,
            p,
            level: 0,
            size: 0,
            head: Rc::new(RefCell::new(MemTableElement {
                entry: MemTableEntry::new(Vec::new(), None, 0),
                next: Vec::new(),
            })),
        }
    }

    fn random_level(&self) -> usize {
        let mut level = 0;
        while rand::random::<f64>() < self.p && level < self.max_level {
            level += 1;
        }
        level
    }

    pub fn delete(&mut self, key: &[u8], timestamp: u128) {
        let (prev_element_ref, path) = self.search(key);
        let prev_element_next = prev_element_ref.borrow().next.get(0).cloned();

        if let Some(element_ref) = prev_element_next {
            if element_ref.borrow().entry.key == key {
                let mut element_mut = element_ref.borrow_mut();
                if let Some(v) = element_mut.entry.value.take() {
                    self.size -= v.len();
                }
                element_mut.entry.deleted = true;
                return;
            }
        }

        let deleted_element = self.insert(path, key, None, timestamp);
        deleted_element.borrow_mut().entry.deleted = true;
    }

    pub fn set(&mut self, key: &[u8], value: Option<&[u8]>, timestamp: u128) {
        let (prev_element_ref, path) = self.search(key);
        let prev_element_next = prev_element_ref.borrow().next.get(0).cloned();

        if let Some(element_ref) = prev_element_next {
            if element_ref.borrow().entry.key == key {
                self.update(element_ref, value);
                return;
            }
        }

        self.insert(path, key, value, timestamp);
    }

    fn update(&mut self, element_ref: MemTableElementRef, value: Option<&[u8]>) {
        let mut element_mut = element_ref.borrow_mut();
        let prev_value_len = element_mut.entry.value.as_ref().map_or(0, |v| v.len());
        let new_value_len = value.as_ref().map_or(0, |v| v.len());

        self.size = self.size - prev_value_len + new_value_len;

        element_mut.entry.value = value.map(|v| v.to_vec());

        element_mut.entry.deleted = false;
    }

    fn insert(
        &mut self,
        mut path: Vec<MemTableElementRef>,
        key: &[u8],
        value: Option<&[u8]>,
        timestamp: u128,
    ) -> MemTableElementRef {
        let value_len = value.map_or(0, |v| v.len());

        self.size += key.len() + value_len + 16 + 1;

        let new_level = self.random_level();
        if new_level > self.level {
            for _ in (self.level + 1)..=new_level {
                path.push(self.head.clone());
            }
            self.level = new_level;
        }

        let new_element = Rc::new(RefCell::new(MemTableElement::new(MemTableEntry::new(
            key.to_vec(),
            value.map(|v| v.to_vec()),
            timestamp,
        ))));
        for lc in 0..=new_level {
            let prev_node_ref = path[lc].clone();

            if let Some(next_node_ref) = prev_node_ref.borrow().next.get(lc).cloned() {
                new_element.borrow_mut().next.push(next_node_ref);
            }

            if prev_node_ref.borrow().next.len() <= lc {
                prev_node_ref.borrow_mut().next.push(new_element.clone());
            } else {
                prev_node_ref.borrow_mut().next[lc] = new_element.clone();
            }
        }
        new_element
    }

    pub fn get(&self, key: &[u8]) -> Option<MemTableEntry> {
        let (prev_element_ref, _) = self.search(key);
        if let Some(element_ref) = prev_element_ref.borrow().next.get(0) {
            let element = element_ref.borrow();

            if element.entry.key == key && !element.entry.deleted {
                return Some(element.entry.clone());
            }
        }
        None
    }

    fn search(&self, key: &[u8]) -> (MemTableElementRef, Vec<MemTableElementRef>) {
        let key = key.to_vec();
        let mut path = vec![self.head.clone(); self.level + 1];
        let mut current = self.head.clone();
        let mut lc = self.level as i64;
        while lc >= 0 {
            let mut move_to_bottom_layer = false;
            let next = current.borrow().next.get(lc as usize).cloned();
            if let Some(next_ref) = next.clone() {
                let next_element = next_ref.borrow();
                if next_element.entry.key < key {
                    current = next_ref.clone();
                } else {
                    move_to_bottom_layer = true;
                }
            } else {
                move_to_bottom_layer = true;
            }
            if move_to_bottom_layer {
                path[lc as usize] = current.clone();
                lc -= 1;
            }
        }
        (current, path)
    } 
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_from_int(i: u32) -> Vec<u8> {
        i.to_be_bytes().to_vec()
    }

    fn calculate_entry_size(key: &[u8], value: &Option<&[u8]>) -> usize {
        let value_len = value.as_ref().map_or(0, |v| v.len());
        key.len() + value_len + 16 + 1
    }

    #[test]
    fn test_set_and_get_single_element() {
        let mut table = MemTable::new(16, 0.5);
        let key = key_from_int(10);
        let value = vec![1, 2, 3];
        let timestamp = 12345;

        table.set(&key, Some(&value), timestamp);

        let expected_size = calculate_entry_size(&key, &Some(&value));
        assert_eq!(table.size, expected_size);

        let entry = table.get(&key).unwrap();
        assert_eq!(entry.value.as_deref(), Some(value.as_slice()));
        assert_eq!(entry.timestamp, timestamp);
    }

    #[test]
    fn test_get_non_existent_key() {
        let mut table = MemTable::new(16, 0.5);
        table.set(&key_from_int(10), Some(&[1]), 100);
        table.set(&key_from_int(30), Some(&[3]), 300);

        assert!(
            table.get(&key_from_int(20)).is_none(),
            "Should return None for a key that doesn't exist"
        );
    }

    #[test]
    fn test_set_multiple_out_of_order() {
        let mut table = MemTable::new(16, 0.5);
        let key10 = key_from_int(10);
        let val10 = vec![1];
        let key20 = key_from_int(20);
        let val20 = vec![2];
        let key30 = key_from_int(30);
        let val30 = vec![3];

        table.set(&key30, Some(&val30), 300);
        table.set(&key10, Some(&val10), 100);
        table.set(&key20, Some(&val20), 200);

        let expected_size = calculate_entry_size(&key10, &Some(val10.as_slice()))
            + calculate_entry_size(&key20, &Some(val20.as_slice()))
            + calculate_entry_size(&key30, &Some(val30.as_slice()));

        assert_eq!(table.size, expected_size);
        assert_eq!(
            table.get(&key10).unwrap().value.as_deref(),
            Some(val10.as_slice())
        );
        assert_eq!(
            table.get(&key20).unwrap().value.as_deref(),
            Some(val20.as_slice())
        );
        assert_eq!(
            table.get(&key30).unwrap().value.as_deref(),
            Some(val30.as_slice())
        );
    }

    #[test]
    fn test_update_existing_key_size_change() {
        let mut table = MemTable::new(16, 0.5);
        let key = key_from_int(25);
        let original_value = vec![1, 1];

        table.set(&key, Some(&original_value), 5555);
        let initial_size = table.size;

        let new_value = vec![2, 2, 2, 2];
        table.set(&key, Some(&new_value), 9999);

        let expected_size = initial_size - original_value.len() + new_value.len();
        assert_eq!(table.size, expected_size, "Size should update correctly");

        let entry = table.get(&key).unwrap();
        assert_eq!(entry.value.as_deref(), Some(new_value.as_slice()));

        assert_eq!(
            entry.timestamp, 5555,
            "Timestamp should not change on update"
        );
    }

    #[test]
    fn test_delete_existing_key() {
        let mut table = MemTable::new(16, 0.5);
        let key = key_from_int(40);
        let value = vec![4, 4, 4];
        table.set(&key, Some(&value), 400);

        let initial_size = table.size;
        table.delete(&key, 401);

        let expected_size = initial_size - value.len();
        assert_eq!(
            table.size, expected_size,
            "Size should decrease by value length"
        );

        assert!(
            table.get(&key).is_none(),
            "Getting a deleted key should return None"
        );
    }

    #[test]
    fn test_delete_non_existent_key() {
        let mut table = MemTable::new(16, 0.5);
        table.set(&key_from_int(10), Some(&[1]), 100);
        let initial_size = table.size;

        let non_existent_key = key_from_int(50);
        table.delete(&non_existent_key, 501);

        let expected_size = initial_size + calculate_entry_size(&non_existent_key, &None);
        assert_eq!(
            table.size, expected_size,
            "Size should increase by deleted entry size"
        );

        assert_eq!(table.get(&non_existent_key), None 
        );
    }

    #[test]
    fn test_set_after_delete() {
        let mut table = MemTable::new(16, 0.5);
        let key = key_from_int(50);
        let original_value = vec![5];

        table.set(&key, Some(&original_value), 500);
        table.delete(&key, 501);

        let size_after_delete = table.size;
        assert!(table.get(&key).is_none());

        let new_value = vec![5, 5, 5];
        table.set(&key, Some(&new_value), 502);

        let expected_size = size_after_delete + new_value.len();
        assert_eq!(table.size, expected_size);

        let entry = table.get(&key).unwrap();
        assert_eq!(entry.value.as_deref(), Some(new_value.as_slice()));
        assert!(!entry.deleted, "Entry should no longer be a deleted");
    }
}
