use std::rc::Rc;
use std::cell::RefCell; 

type ElementRef = Rc<RefCell<Element>>;
struct Element {
    entry: Entry,
    next: Vec<ElementRef> 
}

#[derive(Clone)]
pub struct Entry {
    key: usize,
    value: Vec<u8>,
    tombstone: bool,
}

impl Entry {
    fn new(key: usize, value: Vec<u8>) -> Self {
        Entry { key, value, tombstone: false }
    }
}

pub struct SkipList {
   max_level: usize,    
   p: f64,
   level: usize,
   size: usize,
   head: Option<ElementRef> 
}

impl SkipList {
    pub fn new(max_level: usize, p: f64) -> Self {
        SkipList {
            max_level,
            p,
            level: 0,
            size: 0,
            head: None
        }
    }

    fn random_level(&self) -> usize {
        let mut level = 0;
        while level < self.max_level {
            if rand::random::<f64>() < self.p {
                level += 1;
            } else {
                break;
            }
        }
        level
    }

    pub fn get(&self, key: &usize) -> Option<Entry> {
        self.search(key).map(|element_ref| element_ref.borrow().entry.clone())
    }

    pub fn delete(&self, key: &usize) -> bool {
        if let Some(element_ref) = self.search(key) {
            element_ref.borrow_mut().entry.tombstone = true;
            true
        } else {
            false
        }
    }

    // pub fn insert_or_update(&mut self, key: &usize, value: Vec<u8>)  {
    //     if let Some(element_ref) = self.search(key) {
    //         element_ref.borrow_mut().entry.tombstone = false;
    //         element_ref.borrow_mut().entry.value = value;
    //     } else {
    //         let level = self.random_level();
    //         if level > self.
    //     }
    // }
        

    fn search(&self, key: &usize) -> Option<ElementRef> {
        if self.head.is_none() {
            return None;
        }
        let mut current = self.head.clone().unwrap();
        let mut lc = self.level as i64;
        while lc >= 0 {
            let next = current.borrow().next.get(lc as usize).cloned();
            if let Some(next_ref) = next.clone() {
               let element = next_ref.borrow();
               if element.entry.key > *key {
                 lc -= 1;
               } else {
                 current = next_ref.clone(); 
               }
            } else {
                lc -= 1;    
            }
        }
        if current.borrow().entry.key == *key && !current.borrow().entry.tombstone {
            return Some(current);
        }
        None
    }
} 
