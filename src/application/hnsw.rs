use super::node::{Node, NodeId, LayerNum};
use crate::linalg::vector::Vector;
use crate::numeric::ordered_float::OrderedFloat;
use crate::storage::memtable::MemTable;
use priority_queue::{DoublePriorityQueue, PriorityQueue};
use rand::Rng;
use std::cmp;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};


#[allow(clippy::upper_case_acronyms)]
pub struct HNSW {
    top_layer_num: AtomicUsize,
    entry_id: RwLock<Option<usize>>,
    mem_table: Arc<MemTable>,
}

impl HNSW {
    pub fn new() -> Self {
        Self {
            entry_id: RwLock::new(None),
            top_layer_num: AtomicUsize::new(0),
            mem_table: Arc::new(MemTable::new()),
        }
    }

    fn random_layer(ml: usize) -> usize {
        let mut rng = rand::thread_rng();
        let random_num: f64 = rng.r#gen();
        (-random_num.ln() * ml as f64) as usize
    }

    pub fn insert(
        &mut self,
        vector: Vector,
        m: usize,
        m_max: usize,
        ef_construction: usize,
        ml: usize,
    ) {
        let new_node_id = self.mem_table.insert(vector);
        let new_node_layer = Self::random_layer(ml);

        let entry_id = match self.entry_id {
            Some(id) => id,
            None => {
                self.entry_id = Some(new_node_id);
                self.top_layer_num = new_node_layer;
                return;
            }
        };

        let mut entry_node = self.mem_table.get(&entry_id).unwrap();

        for current_layer_num in ((new_node_layer + 1)..=self.top_layer_num).rev() {
            let new_node_vector = &self.mem_table.get(&new_node_id).unwrap().vector();
            let mut nearest_candidate =
                self.search_layer(new_node_vector, entry_node, 1, current_layer_num);
            entry_node = nearest_candidate.pop_min().unwrap().0;
        }

        for current_layer_num in (0..=cmp::min(self.top_layer_num, new_node_layer)).rev() {
            let new_node_vector = &self.mem_table.get(&new_node_id).unwrap().vector();

            let candidates = self.search_layer(
                new_node_vector,
                entry_node,
                ef_construction,
                current_layer_num,
            );

            let new_node_neighbors = self.select_neighbors(
                new_node_vector,
                candidates.clone(),
                m,
                current_layer_num,
                true,
                true,
            );
            let new_node_neighbors_ids: Vec<NodeId> =
                new_node_neighbors.iter().map(|n| n.id()).collect();

            self.mem_table
                .get_mut(&new_node_id)
                .unwrap()
                .set_neighbor_ids(current_layer_num, new_node_neighbors_ids.clone());

            for neighbor_id in &new_node_neighbors_ids {
                let neighbor_mut = self.mem_table.get_mut(neighbor_id).unwrap();
                let neighbor_connections = neighbor_mut
                    .neighbor_ids(current_layer_num)
                    .unwrap_or(&Vec::new());

                if neighbor_connections.len() < m_max {
                    neighbor_mut.add_neighbor(current_layer_num, new_node_id);
                } else {
                }
            }

            entry_node = new_node_neighbors[0];
        }

        if new_node_layer > self.top_layer_num {
            self.top_layer_num = new_node_layer;
            self.entry_id = Some(new_node_id);
        }
    }

    fn select_neighbors<'a>(
        &'a self,
        query_vector: &Vector,
        mut candidate_pool: DoublePriorityQueue<&'a Node, OrderedFloat>,
        m: usize,
        layer_num: usize,
        extend_candidates: bool,
        keep_pruned_connections: bool,
    ) -> Vec<&'a Node> {
        if extend_candidates {
            let initial_candidates: Vec<&Node> =
                candidate_pool.iter().map(|(node, _)| *node).collect();
            #[allow(clippy::mutable_key_type)]
            let mut visited: HashSet<&Node> = initial_candidates.iter().cloned().collect();

            for candidate in initial_candidates {
                for neighbor_id in candidate.neighbor_ids(layer_num).unwrap_or(&Vec::new()) {
                    let neighbor = self.mem_table.get(neighbor_id).unwrap();
                    if visited.insert(neighbor) {
                        let dist = OrderedFloat(neighbor.vector().squared_distance(query_vector));
                        candidate_pool.push(neighbor, dist);
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
                let dist_to_selected = candidate_node.vector().squared_distance(selected.vector());
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

    fn search_layer<'a>(
        &'a self,
        query_vector: &Vector,
        entry_node: &'a Node,
        ef: usize,
        layer_num: usize,
    ) -> DoublePriorityQueue<&'a Node, OrderedFloat> {
        let mut nearest_neighbors = DoublePriorityQueue::new();
        let mut candidate_heap = PriorityQueue::new();
        let mut visited_nodes = HashSet::new();

        let entry_point_dist = OrderedFloat(entry_node.vector().squared_distance(query_vector));

        visited_nodes.insert(entry_node);
        nearest_neighbors.push(entry_node, entry_point_dist);
        candidate_heap.push(entry_node, Reverse(entry_point_dist));

        while let Some((current_node, Reverse(current_dist))) = candidate_heap.pop() {
            let (_, furthest_neighbor_dist) = nearest_neighbors.peek_max().unwrap();

            if current_dist > *furthest_neighbor_dist && nearest_neighbors.len() >= ef {
                break;
            }

            for neighbor_id in current_node.neighbor_ids(layer_num).unwrap_or(&Vec::new()) {
                let neighbor = self.mem_table.get(neighbor_id).unwrap();
                if visited_nodes.insert(neighbor) {
                    let neighbor_dist =
                        OrderedFloat(neighbor.vector().squared_distance(query_vector));
                    let (_, furthest_dist) = nearest_neighbors.peek_max().unwrap();

                    if neighbor_dist < *furthest_dist || nearest_neighbors.len() < ef {
                        candidate_heap.push(neighbor, Reverse(neighbor_dist));
                        nearest_neighbors.push(neighbor, neighbor_dist);

                        if nearest_neighbors.len() > ef {
                            nearest_neighbors.pop_max();
                        }
                    }
                }
            }
        }
        nearest_neighbors
    }

    pub fn search(&self, query: Vector, k: usize, ef: usize) -> Vec<Vector> {
        if self.entry_id.is_none() {
            return Vec::new();
        }

        let entry_id = self.entry_id.unwrap();
        let mut entry_node = self.mem_table.get(&entry_id).unwrap();

        for current_layer_num in (1..=self.top_layer_num).rev() {
            let mut nearest_candidates =
                self.search_layer(&query, entry_node, 1, current_layer_num);
            entry_node = nearest_candidates.pop_min().unwrap().0;
        }

        let candidates = self.search_layer(&query, entry_node, ef, 0);

        let mut result = candidates
            .into_sorted_iter()
            .map(|(node, _)| node.vector().clone())
            .collect::<Vec<_>>();
        result.truncate(k);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_hnsw() -> (HNSW, Vec<Vector>) {
        let mut hnsw = HNSW::new();
        let vectors = vec![
            Vector::new(vec![0.0, 0.0]),
            Vector::new(vec![1.0, 1.0]),
            Vector::new(vec![8.0, 8.0]),
            Vector::new(vec![10.0, 10.0]),
            Vector::new(vec![12.0, 12.0]),
        ];

        for vector in &vectors {
            hnsw.insert(vector.clone(), 16, 32, 200, 4);
        }
        (hnsw, vectors)
    }

    #[test]
    fn test_insert_creates_entry_point() {
        let (hnsw, _) = setup_hnsw();
        assert!(
            hnsw.entry_id.is_some(),
            "HNSW should have an entry point after insertion."
        );
    }

    #[test]
    fn test_search_returns_correct_neighbors() {
        let (hnsw, _) = setup_hnsw();
        let query = Vector::new(vec![0.5, 0.5]);

        let k = 2;
        let results = hnsw.search(query, k, 100);

        assert_eq!(results.len(), k, "Search should return k results.");

        let expected_neighbor_1 = Vector::new(vec![0.0, 0.0]);
        let expected_neighbor_2 = Vector::new(vec![1.0, 1.0]);

        assert!(
            results.contains(&expected_neighbor_1),
            "Results should contain the nearest neighbor."
        );
        assert!(
            results.contains(&expected_neighbor_2),
            "Results should contain the second nearest neighbor."
        );
    }

    #[test]
    fn test_search_for_exact_match() {
        let (hnsw, _) = setup_hnsw();

        let query = Vector::new(vec![8.0, 8.0]);

        let k = 1;
        let results = hnsw.search(query.clone(), k, 100);

        assert_eq!(results.len(), k);

        assert_eq!(
            results[0], query,
            "The first result should be the exact match."
        );
    }

    #[test]
    fn test_search_on_empty_index() {
        let hnsw = HNSW::new();
        let query = Vector::new(vec![1.0, 1.0]);
        let results = hnsw.search(query, 5, 100);

        assert!(
            results.is_empty(),
            "Search on an empty HNSW should return no results."
        );
    }

    #[test]
    fn test_search_with_k_larger_than_dataset() {
        let (hnsw, vectors) = setup_hnsw();
        let query = Vector::new(vec![0.5, 0.5]);

        let k = vectors.len() + 5;
        let results = hnsw.search(query, k, 100);

        assert_eq!(
            results.len(),
            vectors.len(),
            "Should return all elements if k > dataset size."
        );
    }
}
