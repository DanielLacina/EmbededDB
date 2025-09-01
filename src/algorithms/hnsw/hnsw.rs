use super::node::{HNSWNode, HNSWNodeWrapper};
use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use priority_queue::{DoublePriorityQueue, PriorityQueue};
use std::{cmp::Reverse, result};
use rand::Rng;
use std::collections::{HashMap, HashSet};


#[derive(Debug, Clone)]
pub struct HNSW {
    num_layers: usize,
    entry_point: Option<HNSWNodeWrapper>,
}

impl HNSW {
    pub fn new(num_layers: usize) -> Self {
        Self { num_layers, entry_point: None}
    }

    pub fn insert(&mut self, vector: Vector, m: usize, ef_construction: usize) {
        let num_layers = self.num_layers;
        let query_node = HNSWNode::new(vector, num_layers);
        let entry_point = if let Some(entry_point) = self.entry_point {
                 
        }
        if self.entry_point.is_none() {
        }
        let mut entry_point = entry_point.unwrap().clone();
        let mut rng = rand::thread_rng();
        let random_num = rng.r#gen();
        let l = -f64::ln(random_num) as usize;
        for lc in num_layers..l {
            let mut candidates = self.search_layer(query_node, entry_point.clone(), 1, lc);
            entry_point = candidates.pop_min().unwrap().0;
        }
        for lc in num_layers.min(l)..=0 {
            let candidates = self.search_layer(query_node.clone(), entry_point.clone(), ef_construction, lc);  
            let neighbors = self.select_neighbors(query_node, candidates, m, lc); 
            for neighbor in neighbors {
                neighbor.borrow_mut().insert_neighbors(lc, vec![query_node.clone()]);
            }
            query_node.borrow_mut().insert_neighbors(lc, neighbors);
        }

    }

    fn select_neighbors(&self, query_node: HNSWNodeWrapper, candidates: DoublePriorityQueue<HNSWNodeWrapper, m: usize, OrderedFloat>, m: usize, layer_num: usize) -> Vec<HNSWNodeWrapper> {
        candidates.into_iter().map(|(node, _)| node).collect()
    }



    fn search_layer(
        &self,
        query_node: HNSWNodeWrapper,
        entry_point: HNSWNodeWrapper,
        ef: usize,
        layer_num: usize,
    ) -> DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat> {
        let entry_point_dist = OrderedFloat(
            entry_point
                .borrow()
                .squared_distance(query_node.clone()),
        );
        let mut visited: HashSet<HNSWNodeWrapper> = [entry_point.clone()].into_iter().collect();
        let mut candidates = PriorityQueue::new();
        candidates.push(entry_point.clone(), Reverse(entry_point_dist));
        let mut results = DoublePriorityQueue::new();
        results.push(entry_point.clone(), entry_point_dist);

        while candidates.len() > 0 {
            let (current_candidate, Reverse(current_distance)) = candidates.pop().unwrap();
            let (_, furthest_distance) = results.peek_max().unwrap();
            if current_distance > *furthest_distance {
                break;
            }
            for neighbor in current_candidate.borrow().neighbors(layer_num) {
                if visited.insert(neighbor.clone()) {
                    let neighbor_dist = OrderedFloat(neighbor
                        .borrow()
                        .squared_distance(query_node.clone()));
                    let (_, furthest_distance) = results.peek_max().unwrap();
                    if neighbor_dist < *furthest_distance
                        || results.len() < ef
                    {
                        candidates.push(neighbor.clone(), Reverse(neighbor_dist.clone()));
                        results.push(neighbor.clone(), neighbor_dist);
                        if results.len() > ef {
                            results.pop_max();
                        }
                    }
                }
            }
        }
        results
    }
}
