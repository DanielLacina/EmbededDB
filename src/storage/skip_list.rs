use std::cell::RefCell;
use std::rc::Rc;

type ElementRef = Rc<RefCell<Element>>;
struct Element {
    entry: Entry,
    next: Vec<ElementRef>,
}

#[derive(Clone, Debug)]
pub struct Entry {
    key: usize,
    value: Vec<u8>,
    tombstone: bool,
}

impl PartialEq for Entry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.value == other.value && self.tombstone == other.tombstone
    }
}

impl Entry {
    fn new(key: usize, value: Vec<u8>) -> Self {
        Entry {
            key,
            value,
            tombstone: false,
        }
    }
}

pub struct SkipList {
    max_level: usize,
    p: f64,
    level: usize,
    size: usize,
    head: ElementRef,
}

impl SkipList {
    pub fn new(max_level: usize, p: f64) -> Self {
        SkipList {
            max_level,
            p,
            level: 0,
            size: 0,
            head: Rc::new(RefCell::new(Element {
                entry: Entry::new(0, Vec::new()),
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

    pub fn delete(&mut self, key: &usize) -> bool {
        let (prev_element_ref, _) = self.search(key);
        let prev_element_next = prev_element_ref.borrow().next.get(0).cloned();
        if let Some(element_ref) = prev_element_next {
            if element_ref.borrow().entry.key == *key {
                element_ref.borrow_mut().entry.tombstone = true;
                self.size -= 1;
                return true;
            }
        }
        false
    }

    pub fn update_or_insert(&mut self, key: &usize, value: Vec<u8>) {
        let (prev_element_ref, path) = self.search(key);
        let prev_element_next = prev_element_ref.borrow().next.get(0).cloned();
        if let Some(element_ref) = prev_element_next {
            if element_ref.borrow().entry.key == *key {
                self.update(element_ref, value);
            } else {
                self.insert(path, *key, value);
            }
        } else {
            self.insert(path, *key, value);
        }
    }

    fn update(&mut self, element_ref: ElementRef, value: Vec<u8>) {
        element_ref.borrow_mut().entry.value = value;
        let is_deleted = element_ref.borrow().entry.tombstone;
        if is_deleted {
            self.size += 1;
        }
        element_ref.borrow_mut().entry.tombstone = false;
    }

    fn insert(&mut self, mut path: Vec<ElementRef>, key: usize, value: Vec<u8>) {
        let new_level = self.random_level();
        if new_level > self.level {
            for i in (self.level + 1)..=new_level {
                path.push(self.head.clone());
            }
            self.level = new_level;
        }

        let new_element = Rc::new(RefCell::new(Element {
            entry: Entry::new(key, value),
            next: Vec::new(),
        }));

        for lc in 0..=new_level {
            let prev_node_ref = path[lc].clone();

            let next_node = prev_node_ref.borrow().next.get(lc).cloned();
            if let Some(next_node_ref) = next_node {
                new_element.borrow_mut().next.push(next_node_ref);
            }

            if prev_node_ref.borrow().next.len() <= lc {
                prev_node_ref.borrow_mut().next.push(new_element.clone());
            } else {
                prev_node_ref.borrow_mut().next[lc] = new_element.clone();
            }
        }
        self.size += 1;
    }

    pub fn get(&self, key: &usize) -> Option<Entry> {
        let (prev_element_ref, _) = self.search(key);
        let prev_element_next = prev_element_ref.borrow().next.get(0).cloned();
        if let Some(element_ref) = prev_element_next {
            if element_ref.borrow().entry.key == *key && !element_ref.borrow().entry.tombstone {
                return Some(element_ref.borrow().entry.clone());
            }
        }
        None
    }

    fn search(&self, key: &usize) -> (ElementRef, Vec<ElementRef>) {
        let mut path = vec![self.head.clone(); self.level + 1];
        let mut current = self.head.clone();
        let mut lc = self.level as i64;
        while lc >= 0 {
            let mut move_to_bottom_layer = false;
            let next = current.borrow().next.get(lc as usize).cloned();
            if let Some(next_ref) = next.clone() {
                let next_element = next_ref.borrow();
                if next_element.entry.key < *key {
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

    #[test]
    fn test_insert_and_get_single_element() {
        let mut list = SkipList::new(16, 0.5);
        let key = 10;
        let value = vec![1, 2, 3];
        list.update_or_insert(&key, value.clone());

        assert_eq!(list.size, 1);
        assert_eq!(list.get(&key).map(|e| e.value), Some(value));
    }

    #[test]
    fn test_get_non_existent_key() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&10, vec![1]);
        list.update_or_insert(&30, vec![3]);

        assert_eq!(
            list.get(&20),
            None,
            "Should return None for a key that doesn't exist"
        );
    }

    #[test]
    fn test_get_from_empty_list() {
        let list = SkipList::new(16, 0.5);
        assert_eq!(
            list.get(&100),
            None,
            "Should not panic and should return None from an empty list"
        );
    }

    #[test]
    fn test_insert_multiple_out_of_order() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&30, vec![3]);
        list.update_or_insert(&10, vec![1]);
        list.update_or_insert(&20, vec![2]);

        assert_eq!(list.size, 3);
        assert_eq!(list.get(&10).map(|e| e.value), Some(vec![1]));
        assert_eq!(list.get(&20).map(|e| e.value), Some(vec![2]));
        assert_eq!(list.get(&30).map(|e| e.value), Some(vec![3]));
    }

    #[test]
    fn test_update_existing_key() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&25, vec![1, 1]);

        let new_value = vec![2, 2];
        list.update_or_insert(&25, new_value.clone());

        assert_eq!(list.size, 1, "Size should not increase on update");
        assert_eq!(list.get(&25).map(|e| e.value), Some(new_value));
    }

    #[test]
    fn test_search_finds_correct_predecessor() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&10, vec![]);
        list.update_or_insert(&30, vec![]);

        let (predecessor, _) = list.search(&20);
        assert_eq!(
            predecessor.borrow().entry.key,
            10,
            "Predecessor for key 20 should be 10"
        );

        let (predecessor_large, _) = list.search(&100);
        assert_eq!(
            predecessor_large.borrow().entry.key,
            30,
            "Predecessor for key 100 should be 30"
        );
    }

    #[test]
    fn test_delete_existing_key() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&40, vec![4]);
        assert_eq!(list.size, 1);

        assert!(
            list.delete(&40),
            "Delete should return true for an existing key"
        );
        assert_eq!(list.size, 0, "Size should decrease after deletion");
        assert_eq!(
            list.get(&40),
            None,
            "Getting a deleted key should return None"
        );
    }

    #[test]
    fn test_delete_non_existent_key() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&40, vec![4]);

        assert!(
            !list.delete(&50),
            "Delete should return false for a non-existent key"
        );
        assert_eq!(
            list.size, 1,
            "Size should not change when deleting a non-existent key"
        );
    }

    #[test]
    fn test_reinserting_deleted_key() {
        let mut list = SkipList::new(16, 0.5);
        list.update_or_insert(&50, vec![5]);
        list.delete(&50);
        assert_eq!(list.get(&50), None);
        assert_eq!(list.size, 0);

        list.update_or_insert(&50, vec![5, 5]);
        assert_eq!(list.size, 1);
        let entry = list.get(&50).unwrap();
        assert_eq!(entry.value, vec![5, 5]);
        assert!(
            !entry.tombstone,
            "Re-inserted entry should not be a tombstone"
        );
    }
}
