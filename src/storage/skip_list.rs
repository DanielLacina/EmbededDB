use std::rc::Rc;
use std::cell::RefCell; 

type ElementRef = Rc<RefCell<Element>>;
struct Element {
    entry: Entry,
    next: Vec<ElementRef> 
}

#[derive(Clone)]
struct Entry {
    key: usize,
    value: Vec<u8>,
    tombstone: bool,
}

impl Entry {
    fn new(key: usize, value: Vec<u8>) -> Self {
        Entry { key, value, tombstone: false }
    }
}

struct SkipList {
   max_level: usize,    
   p: f64,
   level: usize,
   size: usize,
   head: Option<ElementRef> 
}

impl SkipList {
    fn new(max_level: usize, p: f64) -> Self {
        SkipList {
            max_level,
            p,
            level: 0,
            size: 0,
            head: None
        }
    }

    fn search(&self, key: &usize) -> Option<Entry> {
        if self.head.is_none() {
            return None;
        }
        let mut current = self.head.clone().unwrap();
        let mut lc = self.level;
        while lc > 0 {
            let next = current.borrow().next.get(lc).cloned();
            if let Some(node_ref) = next.clone() {
               let node = node_ref.borrow();
               if node.entry.key == *key {
                 return Some(node.entry.clone());
               } else if node.entry.key > *key {
                 lc -= 1;
               } else {
                 current = node_ref.clone(); 
               }
            } else {
                lc -= 1;    
            }
        }
        None
    }
} 
