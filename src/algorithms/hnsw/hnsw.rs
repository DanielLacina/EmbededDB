use super::node::{HNSWNode, HNSWNodeWrapper};
use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use core::hash;
use priority_queue::{DoublePriorityQueue, PriorityQueue};
use rand::Rng;
use std::cmp;
use std::collections::{HashMap, HashSet};
use std::{cmp::Reverse, hash::Hash, result};

#[derive(Debug, Clone)]
pub struct HNSW {
    top_layer_num: usize,
    entry_point: Option<HNSWNodeWrapper>,
}

impl HNSW {
    pub fn new() -> Self {
        Self {
            entry_point: None,
            top_layer_num: 0,
        }
    }

    fn random_layer() -> usize {
        let mut rng = rand::thread_rng();
        let random_num = rng.r#gen();
        let l = -f64::ln(random_num) as usize;
        l
    }

    pub fn search(&mut self, vector: Vector, ef: usize) -> Vec<Vector> {
        let query_node = HNSWNode::new(vector);
        let mut entry_point = self.entry_point.clone().unwrap();

        for current_layer_num in (1..=self.top_layer_num).rev() {
            let mut nearest_candidates =
                self.search_layer(query_node.clone(), entry_point, 1, current_layer_num);
            entry_point = nearest_candidates.pop_min().unwrap().0;
        }

        let candidates = self.search_layer(query_node.clone(), entry_point, ef, 0);
        candidates
            .into_sorted_iter()
            .map(|(node, _)| node.borrow().vector().clone())
            .collect()
    }

    pub fn insert(&mut self, vector: Vector, m: usize, m_max: usize, ef_construction: usize) {
        let new_node = HNSWNode::new(vector);
        let new_node_layer = Self::random_layer();

        if self.entry_point.is_none() {
            self.entry_point = Some(new_node);
            self.top_layer_num = new_node_layer;
            return;
        }

        let current_top_layer = self.top_layer_num;
        let mut entry_point = self.entry_point.clone().unwrap();

        for current_layer_num in ((new_node_layer + 1)..=current_top_layer).rev() {
            let mut nearest_candidate =
                self.search_layer(new_node.clone(), entry_point, 1, current_layer_num);
            entry_point = nearest_candidate.pop_min().unwrap().0;
        }

        let insertion_top_layer = cmp::min(self.top_layer_num, new_node_layer);
        for current_layer_num in (0..=insertion_top_layer).rev() {
            let candidates = self.search_layer(
                new_node.clone(),
                entry_point.clone(),
                ef_construction,
                current_layer_num,
            );

            let new_node_neighbors = self.select_neighbors(
                new_node.clone(),
                candidates.clone(),
                m,
                current_layer_num,
                true,
                true,
            );

            new_node
                .borrow_mut()
                .set_neighbors(current_layer_num, new_node_neighbors.clone());

            for neighbor in &new_node_neighbors {
                let mut neighbor_mut = neighbor.borrow_mut();

                if neighbor_mut.neighbors(current_layer_num).len() == m_max {
                    let updated_neighbors = self.select_neighbors(
                        neighbor.clone(),
                        candidates.clone(),
                        m,
                        current_layer_num,
                        true,
                        true,
                    );
                    neighbor_mut.set_neighbors(current_layer_num, updated_neighbors);
                } else {
                    neighbor_mut.add_neighbor(current_layer_num, new_node.clone());
                }
            }

            entry_point = new_node_neighbors[0].clone();
        }

        if new_node_layer > self.top_layer_num {
            self.top_layer_num = new_node_layer;
            self.entry_point = Some(new_node);
        }
    }

    fn select_neighbors(
        &self,
        query_node: HNSWNodeWrapper,
        mut candidate_pool: DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat>,
        m: usize,
        layer_num: usize,
        extend_candidates: bool,
        keep_pruned_connections: bool,
    ) -> Vec<HNSWNodeWrapper> {
        if extend_candidates {
            let initial_candidates: Vec<_> = candidate_pool
                .iter()
                .map(|(node, _)| node.clone())
                .collect();
            let mut visited: HashSet<_> = initial_candidates.iter().cloned().collect();

            for candidate in initial_candidates {
                for neighbor in candidate.borrow().neighbors(layer_num) {
                    if visited.insert(neighbor.clone()) {
                        let dist =
                            OrderedFloat(neighbor.borrow().squared_distance(query_node.clone()));
                        candidate_pool.push(neighbor.clone(), dist);
                    }
                }
            }
        }

        let mut selected_neighbors = Vec::with_capacity(m);
        let mut pruned_connections = DoublePriorityQueue::new();

        while let Some((candidate_node, candidate_dist)) = candidate_pool.pop_min() {
            if selected_neighbors.len() >= m {
                break;
            }

            if selected_neighbors.is_empty() {
                selected_neighbors.push(candidate_node);
                continue;
            }

            let is_diverse_candidate = selected_neighbors.iter().all(|selected| {
                let dist_to_selected = candidate_node.borrow().squared_distance(selected.clone());
                dist_to_selected >= candidate_dist.0
            });

            if is_diverse_candidate {
                selected_neighbors.push(candidate_node);
            } else {
                pruned_connections.push(candidate_node, candidate_dist);
            }
        }

        if keep_pruned_connections {
            while selected_neighbors.len() < m {
                if let Some((best_pruned, _)) = pruned_connections.pop_min() {
                    selected_neighbors.push(best_pruned);
                } else {
                    break;
                }
            }
        }

        selected_neighbors
    }

    fn search_layer(
        &self,
        query_node: HNSWNodeWrapper,
        entry_point: HNSWNodeWrapper,
        ef: usize,
        layer_num: usize,
    ) -> DoublePriorityQueue<HNSWNodeWrapper, OrderedFloat> {
        let mut nearest_neighbors = DoublePriorityQueue::new();

        let mut candidate_heap = PriorityQueue::new();

        let mut visited_nodes = HashSet::new();

        let entry_point_dist =
            OrderedFloat(entry_point.borrow().squared_distance(query_node.clone()));

        visited_nodes.insert(entry_point.clone());
        nearest_neighbors.push(entry_point.clone(), entry_point_dist);
        candidate_heap.push(entry_point.clone(), Reverse(entry_point_dist));

        while let Some((current_node, Reverse(current_dist))) = candidate_heap.pop() {
            let (_, furthest_neighbor_dist) = nearest_neighbors.peek_max().unwrap();

            if current_dist > *furthest_neighbor_dist && nearest_neighbors.len() >= ef {
                break;
            }

            for neighbor in current_node.borrow().neighbors(layer_num) {
                if visited_nodes.insert(neighbor.clone()) {
                    let neighbor_dist =
                        OrderedFloat(neighbor.borrow().squared_distance(query_node.clone()));
                    let (_, furthest_dist) = nearest_neighbors.peek_max().unwrap();

                    if neighbor_dist < *furthest_dist || nearest_neighbors.len() < ef {
                        candidate_heap.push(neighbor.clone(), Reverse(neighbor_dist));

                        nearest_neighbors.push(neighbor.clone(), neighbor_dist);

                        if nearest_neighbors.len() > ef {
                            nearest_neighbors.pop_max();
                        }
                    }
                }
            }
        }

        nearest_neighbors
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] 
    fn test_hnsw_insert() {
        let mut hnsw = HNSW::new();
        let vectors = vec![
            Vector::new(vec![0.0, 0.0]),
            Vector::new(vec![1.0, 1.0]),
            Vector::new(vec![2.0, 2.0]),
            Vector::new(vec![3.0, 3.0]),
            Vector::new(vec![4.0, 4.0]),
        ];

        for vector in &vectors {
            hnsw.insert(vector.clone(), 2, 4, 3);
        }

        assert_eq!(hnsw.top_layer_num >= 0, true);
        assert!(hnsw.entry_point.is_some());
    }

    #[test] 
    fn test_hnsw_search() {
        let mut hnsw = HNSW::new();
        let vectors = vec![
            Vector::new(vec![0.0, 0.0]),
            Vector::new(vec![1.0, 1.0]),
            Vector::new(vec![2.0, 2.0]),
            Vector::new(vec![3.0, 3.0]),
            Vector::new(vec![4.0, 4.0]),
        ];

        for vector in &vectors {
            hnsw.insert(vector.clone(), 2, 4, 3);
        }

        let query = Vector::new(vec![0.5, 0.5]);
        let results = hnsw.search(query, 2);
        println!("{:?}", results);

        assert_eq!(results.len(), 2);
    }
}
