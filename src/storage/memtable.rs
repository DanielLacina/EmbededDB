use super::node::{Node, NodeId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;
use std::sync::Arc;
use dashmap::DashMap;
use crate::linalg::vector::Vector;


pub struct MemTable {
    nodes: DashMap<NodeId, Arc<RwLock<Node>>>,
    next_node_id: AtomicUsize
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            nodes: DashMap::new(),
            next_node_id: AtomicUsize::new(0),
        }
    }

    pub fn insert(&self, vector: Vector) -> NodeId {
        let node_id = self.next_node_id.fetch_add(1, Ordering::SeqCst);

        let new_node = Node::new(node_id, vector);

        self.nodes.insert(node_id, Arc::new(RwLock::new(new_node)));

        node_id
    }

    pub fn get(&self, node_id: &NodeId) -> Option<Arc<RwLock<Node>>> {
        self.nodes.get(node_id).map(|node_ref| node_ref.clone())
    }
}
