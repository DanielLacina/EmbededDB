use crate::linalg::vector::Vector;
use std::hash::Hash;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap};

pub struct HNSWNodeWrapper(pub HNSWNodeRef);

impl Hash for HNSWNodeWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Rc::as_ptr(&self.0).hash(state);
    }
}

impl Clone for HNSWNodeWrapper {
    fn clone(&self) -> Self {
        HNSWNodeWrapper(self.0.clone())
    }
}

impl PartialEq for HNSWNodeWrapper {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for HNSWNodeWrapper {
}

pub type HNSWNodeRef = Rc<RefCell<HNSWNode>>; 

pub struct HNSWNode {
    vector: Vector,
    neighbors: HashMap<usize, Vec<HNSWNodeRef>>,
}

impl HNSWNode {
    pub fn new(vector: &Vector) -> HNSWNodeRef {
        let node = Self {
            vector: vector.clone(),
            neighbors: HashMap::new(),  
        };
        Rc::new(RefCell::new(node))

    }
    pub fn neighbors(&self, layer_num: usize) -> &Vec<HNSWNodeRef> {
        &self.neighbors.get(&layer_num).unwrap()
    }

    pub fn squared_distance(&self, other: HNSWNodeRef) -> f64 {
        self.vector.clone().squared_distance(&other.borrow().vector)
    } 
}

impl Hash for HNSWNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.vector.hash(state);
    }
}