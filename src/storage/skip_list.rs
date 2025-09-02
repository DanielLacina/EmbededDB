use std::rc::Rc;

struct SkipList {
   max_level: usize,    
   p: f64,
   level: usize,
   size: usize,
   head: Element 
}

struct Element {
    entry: Entry,
    next: Vec<Element> 
}

struct Entry {
    key: String,
    value: Vec<u8>,
    tombstone: bool
}
