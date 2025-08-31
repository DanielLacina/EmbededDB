use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use std::hash::Hash;
use std::rc::Rc;
use std::cell::RefCell;
use rand::Rng;
use std::collections::{HashSet, HashMap, BinaryHeap};
use std::cmp::Reverse;

type HNSWNodeRef = Rc<RefCell<HNSWNode>>; 

struct HNSWNode {
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

pub struct HNSW {
    layers: HashMap<usize, Vec<HNSWNodeRef>>, 
}

impl HNSW {
    pub fn new(num_layers: usize) -> Self {
        let mut layers = HashMap::new();
        for i in 0..num_layers {
            layers.insert(i, Vec::new());
        }
        Self {
            layers
        }
    }

    fn num_layers(&self) -> usize {
        self.layers.len()
    }

    pub fn insert(&self, vector: &Vector) {
        let node_to_insert = HNSWNode::new(vector);  
        let mut nearest_elements = Vec::new(); 
        let mut top_layer = self.num_layers();  
        let entry_point = self.layers.get(&top_layer).unwrap().get(0);
        if entry_point.is_none() {
            for layer_num in 0..top_layer {
                self.layers.get_mut(&layer_num).unwrap().push(node_to_insert.clone());
            }
        }
        let entry_point = entry_point.unwrap();
        let mut rng = rand::thread_rng();
        let random_num = rng.r#gen();
        let layer = -f64::ln(random_num);
        for l in 0..top_layer {
        }   
    }

    pub fn search_layer(&self, node_to_insert: HNSWNodeRef, entry_point: HNSWNodeRef, num_nearest_neighbors: usize, layer_num: usize) {
        let mut visited = HashSet::new();
        visited.insert(node_to_insert.clone());
        let entry_point_distance = OrderedFloat(node_to_insert.borrow().squared_distance(entry_point));
        let mut candidates = BinaryHeap::new(); 
        candidates.push(Reverse((entry_point_distance, &entry_point_vector)));
        let mut nearest_neighbors = BinaryHeap::new();
        nearest_neighbors.push(Reverse((entry_point_distance, &entry_point_vector)));
        while candidates.len() > 0 {
            let nearest_candidate = candidates.pop().unwrap();
            let furthest_nearest_neighbor = nearest_neighbors.pop().unwrap();
        }
    }

    



    // pub fn search_layer(&self) {
    //     let mut visited = HashSet::new();
    //     visited.insert(self.entry_point.clone());
    //     let mut candidates = BinaryHeap::new();
    //     candidates.push(self.entry_point.clone());
    //     let mut nearest_neighbors = BinaryHeap::new();  
    //     candidates.h(self.entry_point.clone());

    //     while candidates.len() > 0 {
    //         let nearest_candidate = candidates.pop().unwrap(); 
    //         let furthest_nearest_neighbor =  
    //     }
        
    // } 
    
}
