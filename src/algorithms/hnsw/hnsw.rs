use super::node::{HNSWNode, HNSWNodeWrapper};
use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use core::hash;
use priority_queue::{DoublePriorityQueue, PriorityQueue};
use rand::Rng;
use std::collections::{HashMap, HashSet};
use std::{cmp::Reverse, hash::Hash, result};

#[derive(Debug, Clone)]
pub struct HNSW {
    top_layer: usize,
    entry_point: Option<HNSWNodeWrapper>,
}

impl HNSW {
    pub fn new() -> Self {
        Self {
            entry_point: None,
            top_layer: 0,
        }
    }

    fn random_layer() -> usize {
        let mut rng = rand::thread_rng();
        let random_num = rng.r#gen();
        let l = -f64::ln(random_num) as usize;
        l
    }

    pub fn insert(&mut self, vector: Vector, m: usize, m_max: usize, ef_construction: usize) {
        let top_layer = self.top_layer;
        let l = Self::random_layer();
        let query_node = HNSWNode::new(vector);
        let mut entry_point = if let Some(entry_point) = self.entry_point.clone() {
            entry_point
        } else {
            self.entry_point = Some(query_node);
            self.top_layer = l;
            return;
        };
        for lc in top_layer..l {
            let mut candidates = self.search_layer(query_node.clone(), entry_point.clone(), 1, lc);
            entry_point = candidates.pop_min().unwrap().0;
        }
        for lc in (top_layer).min(l)..=0 {
            let candidates =
                self.search_layer(query_node.clone(), entry_point.clone(), ef_construction, lc);
            let neighbors = self.select_neighbors(query_node.clone(), candidates.clone(), m, lc, true, true);
            for neighbor in neighbors.iter() {
                neighbor
                    .borrow_mut()
                    .insert_neighbors(lc, vec![query_node.clone()]);
                let neighbor_neighbors_len = neighbor.borrow().neighbors(lc).len();
                if neighbor_neighbors_len > m_max {
                    let new_neighbors = self.select_neighbors(neighbor.clone(), candidates.clone(), m, lc, true, true);
                    neighbor.borrow_mut().insert_neighbors(lc, new_neighbors);
                }
            }
            query_node.borrow_mut().insert_neighbors(lc, neighbors);
        }
        if l > top_layer {
            self.entry_point = Some(query_node);
            self.top_layer = l;
        }
    }

    fn select_neighbors(
        &self,
        query_node: HNSWNodeWrapper,
        candidates: DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat>,
        m: usize,
        lc: usize,
        extend_candidates: bool,
        keep_pruned_connections: bool,
    ) -> Vec<HNSWNodeWrapper> {
        let mut results = Vec::new();
        let mut candidates = candidates;
        if extend_candidates {
            let mut visited = HashSet::new();
            let mut initial_candidates = Vec::new();
            for (candidate, _) in candidates.iter() {
                initial_candidates.push(candidate.clone());
                visited.insert(candidate.clone());
            }
            for candidate in initial_candidates {
                for neighbor in candidate.borrow().neighbors(lc) {
                    if !visited.contains(neighbor) {
                        visited.insert(neighbor.clone());
                        candidates.push(
                            neighbor.clone(),
                            OrderedFloat(neighbor.borrow().squared_distance(query_node.clone())),
                        );
                    }
                }
            }
        }
        let mut discarded_candidates = PriorityQueue::new();
        while candidates.len() > 0 && results.len() < m {
            let (current_candidate, current_distance) = candidates.pop_min().unwrap();
            if results.len() == 0 {
                results.push(current_candidate.clone());
            } else {
                let mut is_good_candidate = true; 
                for result in results.iter() {
                    let dist = current_candidate
                        .borrow()
                        .squared_distance(result.clone());
                    if dist < current_distance.0 {
                        is_good_candidate = false;
                        break;
                    } 
                }  
                if is_good_candidate {
                    results.push(current_candidate.clone());
                } else {
                    discarded_candidates.push(Reverse(current_candidate.clone()), current_distance);
                }
            }
        }
        if keep_pruned_connections {
            while discarded_candidates.len() > 0 && results.len() < m {
                let (Reverse(nearest_discarded), _) =
                    discarded_candidates.pop().unwrap();
                results.push(nearest_discarded.clone());
            }
        }
        results
    }

    fn search_layer(
        &self,
        query_node: HNSWNodeWrapper,
        entry_point: HNSWNodeWrapper,
        ef: usize,
        layer_num: usize,
    ) -> DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat> {
        let entry_point_dist =
            OrderedFloat(entry_point.borrow().squared_distance(query_node.clone()));
        let mut visited: HashSet<HNSWNodeWrapper> = [entry_point.clone()].into_iter().collect();
        let mut candidates = PriorityQueue::new();
        candidates.push(entry_point.clone(), Reverse(entry_point_dist));
        let mut results = DoublePriorityQueue::new();
        results.push(entry_point.clone(), entry_point_dist);

        while candidates.len() > 0 {
            let (current_candidate, Reverse(current_distance)) = candidates.pop().unwrap();
            let (_, furthest_distance) = results.peek_max().unwrap();
            if current_distance > *furthest_distance && results.len() >= ef {
                break;
            }
            for neighbor in current_candidate.borrow().neighbors(layer_num) {
                if visited.insert(neighbor.clone()) {
                    let neighbor_dist =
                        OrderedFloat(neighbor.borrow().squared_distance(query_node.clone()));
                    let (_, furthest_distance) = results.peek_max().unwrap();
                    if neighbor_dist < *furthest_distance || results.len() < ef {
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
