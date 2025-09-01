use super::node::{HNSWNode, HNSWNodeWrapper};
use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use priority_queue::{DoublePriorityQueue, PriorityQueue};
use core::hash;
use std::{cmp::Reverse, result};
use rand::Rng;
use std::collections::{HashMap, HashSet};


#[derive(Debug, Clone)]
pub struct HNSW {
    max_layer: usize,
    entry_point: Option<HNSWNodeWrapper>,
}

impl HNSW {
    pub fn new() -> Self {
        Self { entry_point: None, max_layer: 0 }
    }

    pub fn insert(&mut self, vector: Vector, m: usize, m_max: usize, ef_construction: usize) {
        let max_layer = self.max_layer;
        let query_node = HNSWNode::new(vector);
        let mut rng = rand::thread_rng();
        let random_num = rng.r#gen();
        let l = -f64::ln(random_num) as usize;
        let mut entry_point = if let Some(entry_point) = self.entry_point.clone() {
            entry_point
        } else {
            self.entry_point = Some(query_node);
            self.max_layer = l;
            return;
        };
        for lc in max_layer..l {
            let (mut candidates, _) = self.search_layer(query_node.clone(), entry_point.clone(), 1, lc);
            entry_point = candidates.pop_min().unwrap().0;
        }
        for lc in (max_layer).min(l)..=0 {
            let (candidates, hashed_candidates) = self.search_layer(query_node.clone(), entry_point.clone(), ef_construction, lc);  
            let neighbors = self.select_neighbors(query_node.clone(), candidates, hashed_candidates, m, lc); 
            for neighbor in neighbors.iter() {
                neighbor.borrow_mut().insert_neighbors(lc, vec![query_node.clone()]);
                let neighbor_neighbors = neighbor.borrow().neighbors(lc);
                if neighbor_neighbors.len() > m_max {
                   let new_neighbors = self.select_neighbors(query_node, candidates, m, lc);  
                }
            }
            query_node.borrow_mut().insert_neighbors(lc, neighbors);
        }
        if l > max_layer {
            self.entry_point = Some(query_node);
            self.max_layer = l;
        }

    }

    fn select_neighbors(&self, query_node: HNSWNodeWrapper, candidates: DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat>, hashed_candidates: HashSet<HNSWNodeWrapper>, m: usize, layer_num: usize) -> Vec<HNSWNodeWrapper> {
        let mut results = PriorityQueue::new();
        let mut hashed_candidates = hashed_candidates; 
        let mut candidates = candidates;
        while let Some(candidate) = candidates.pop_min() {
            for neighbor in candidate.borrow().neighbors(layer_num) {
                if !hashed_candidates.contains(neighbor) {
                    hashed_candidates.insert(neighbor.clone());    
                    candidates.push(neighbor.clone(), OrderedFloat(neighbor.borrow().squared_distance(query_node.clone())));
                }
            }
        }
        let mut discarded_candidates = PriorityQueue::new();
        while candidates.len() > 0 && results.len() < m {
            let (current_candidate, current_distance) = candidates.peek_min().unwrap();     
            if results.len() == 0 {
                results.push(Reverse(current_candidate.clone()), current_distance);
            } else {
                let (_, nearest_distance) = results.peek().unwrap();  
                if current_distance < nearest_distance {
                    results.push(Reverse(current_candidate.clone()), current_distance);
                } else {
                    discarded_candidates.push(Reverse(current_candidate.clone()), current_distance); 
                }
            }
            while discarded_candidates.len() > 0 && results.len() < m{
                let (nearest_discarded, nearest_discared_distance) = discarded_candidates.pop().unwrap();
                results.push(nearest_discarded.clone(), nearest_discared_distance);
            }
        }  
        results.into_iter().map(|(Reverse(node), _)| node).collect()
    }



    fn search_layer(
        &self,
        query_node: HNSWNodeWrapper,
        entry_point: HNSWNodeWrapper,
        ef: usize,
        layer_num: usize,
    ) -> (DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat>, HashSet<HNSWNodeWrapper>) {
        let entry_point_dist = OrderedFloat(
            entry_point
                .borrow()
                .squared_distance(query_node.clone()),
        );
        let mut visited: HashSet<HNSWNodeWrapper> = [entry_point.clone()].into_iter().collect();
        let mut candidates = PriorityQueue::new();
        candidates.push(entry_point.clone(), Reverse(entry_point_dist));
        let mut results = DoublePriorityQueue::new();
        let mut hashed_results = HashSet::new(); 
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
                        hashed_results.insert(neighbor.clone());
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
