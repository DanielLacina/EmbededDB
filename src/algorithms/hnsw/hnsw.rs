use super::node::{HNSWNode, HNSWNodeWrapper};
use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};

#[derive(Debug, Clone)]
struct HNSWNodeDistancePair {
    pub distance: OrderedFloat,
    pub node: HNSWNodeWrapper,
}

impl HNSWNodeDistancePair {
    fn new(distance: OrderedFloat, node: HNSWNodeWrapper) -> Self {
        Self { distance, node }
    }
}

impl PartialEq for HNSWNodeDistancePair {
    fn eq(&self, other: &Self) -> bool {
        self.distance == other.distance
    }
}

impl Eq for HNSWNodeDistancePair {}

impl PartialOrd for HNSWNodeDistancePair {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&other.distance)
    }
}

impl Ord for HNSWNodeDistancePair {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance.cmp(&other.distance)
    }
}

#[derive(Debug, Clone)]
pub struct HNSW {
    layers: HashMap<usize, Vec<HNSWNodeWrapper>>,
}

impl HNSW {
    pub fn new(num_layers: usize) -> Self {
        let mut layers = HashMap::new();
        for i in 0..num_layers {
            layers.insert(i, Vec::new());
        }
        Self { layers }
    }

    fn num_layers(&self) -> usize {
        self.layers.len()
    }

    pub fn insert(&self, vector: Vector) {
        let node_to_insert = HNSWNodeWrapper(HNSWNode::new(vector));
        let mut nearest_elements = Vec::new();
        let mut top_layer = self.num_layers();
        let entry_point = self.layers.get(&top_layer).unwrap().get(0);
        if entry_point.is_none() {
            for layer_num in 0..top_layer {
                self.layers
                    .get_mut(&layer_num)
                    .unwrap()
                    .push(node_to_insert.clone());
            }
        }
        let entry_point = entry_point.unwrap();
        let mut rng = rand::thread_rng();
        let random_num = rng.r#gen();
        let layer = -f64::ln(random_num);
        for l in 0..top_layer {}
    }

    fn search_layer(
        &self,
        query_node: HNSWNodeWrapper,
        entry_point: HNSWNodeWrapper,
        ef: usize,
        layer_num: usize,
    ) -> Vec<HNSWNodeWrapper>{
        let entry_point_dist = OrderedFloat(
            entry_point
                .borrow()
                .squared_distance(query_node.clone()),
        );
        let entry_point_pair =
            HNSWNodeDistancePair::new(entry_point_dist, entry_point.clone());
        let mut visited: HashSet<HNSWNodeWrapper> = [entry_point.clone()].into_iter().collect();
        let mut candidates = BinaryHeap::from([Reverse(entry_point_pair.clone())]);
        let mut results = BinaryHeap::from([entry_point_pair]);

        while candidates.len() > 0 {
            let current_candidate = candidates.pop().unwrap().0;
            let furthest_result = results.peek().unwrap();
            if current_candidate.distance > furthest_result.distance {
                break;
            }
            for neighbor in current_candidate.node.borrow().neighbors(layer_num) {
                if visited.insert(neighbor.clone()) {
                    let neighbor_dist = neighbor
                        .borrow()
                        .squared_distance(query_node.clone());
                    let furthest_result = results.peek().unwrap();
                    if neighbor_dist < furthest_result.distance.0
                        || results.len() < ef
                    {
                        let new_pair = HNSWNodeDistancePair::new(
                            OrderedFloat(neighbor_dist),
                            neighbor.clone(),
                        );
                        candidates.push(Reverse(new_pair.clone()));
                        results.push(new_pair);
                        if results.len() > ef {
                            results.pop();
                        }
                    }
                }
            }
        }
        results.iter().map(|result| result.node.clone()).collect()
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
