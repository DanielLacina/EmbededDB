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
    pub fn new(vector: Vector) -> HNSWNodeWrapper {
        let node = Self {
            vector: vector,
            neighbors: HashMap::new(),
        };
        HNSWNodeWrapper(Rc::new(RefCell::new(node)))
    }

    pub fn neighbors(&self, layer_num: usize) -> &[HNSWNodeWrapper] {
        self.neighbors
            .get(&layer_num)
            .map_or(&[], |v| v.as_slice())
    } 

    pub fn squared_distance(&self, other: HNSWNodeWrapper) -> f64 {
        self.vector.squared_distance(&other.borrow().vector)
    }

    pub fn vector(&self) -> &Vector {
        &self.vector
    }

    pub fn set_neighbors(&mut self, layer_num: usize, new_neighbors: Vec<HNSWNodeWrapper>) {
        self.neighbors.insert(layer_num, new_neighbors);
    }

    pub fn add_neighbor(&mut self, layer_num: usize, neighbor: HNSWNodeWrapper) {
        self.neighbors.entry(layer_num).or_default().push(neighbor);
    }
}

impl Hash for HNSWNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.vector.hash(state);
    }
}
