use crate::linalg::vector::Vector;
use std::cell::{Ref, RefCell, RefMut};
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

pub type HNSWNodeRef = Rc<RefCell<HNSWNode>>;

#[derive(Debug, Clone)]
pub struct HNSWNodeWrapper(pub HNSWNodeRef);

impl HNSWNodeWrapper {
    pub fn borrow(&self) -> Ref<HNSWNode> {
        self.0.borrow()
    }

    pub fn borrow_mut(&self) -> RefMut<HNSWNode> {
        self.0.borrow_mut()
    }
}

impl Hash for HNSWNodeWrapper {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Rc::as_ptr(&self.0).hash(state);
    }
}

impl PartialEq for HNSWNodeWrapper {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for HNSWNodeWrapper {}

#[derive(Debug, Clone)]
pub struct HNSWNode {
    vector: Vector,
    neighbors: HashMap<usize, Vec<HNSWNodeWrapper>>,
}

impl HNSWNode {
    pub fn new(vector: Vector, num_layers: usize) -> HNSWNodeWrapper {
        let mut neighbors = HashMap::new();
        for i in 0..(num_layers + 1) {
            neighbors.insert(i, Vec::new());
        }
        let node = Self {
            vector: vector,
            neighbors
        };
        HNSWNodeWrapper(Rc::new(RefCell::new(node)))
    }

    pub fn neighbors(&self, layer_num: usize) -> &Vec<HNSWNodeWrapper> {
        self.neighbors.get(&layer_num).unwrap()
    }

    pub fn squared_distance(&self, other: HNSWNodeWrapper) -> f64 {
        self.vector.squared_distance(&other.borrow().vector)
    }

    pub fn insert_neighbors(&mut self, layer_num: usize, new_neighbors: Vec<HNSWNodeWrapper>) {
        let neighbors = self.neighbors.get_mut(&layer_num).unwrap();    
        neighbors.extend(new_neighbors);
    }
}

impl Hash for HNSWNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.vector.hash(state);
    }
}
