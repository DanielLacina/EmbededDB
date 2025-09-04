use crate::linalg::vector::Vector;
use std::cell::RefCell;
use std::collections::HashMap;
use std::hash::Hash;
use std::rc::Rc;

pub type NodeId = usize;
pub type LayerNum = usize;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Node {
    id: NodeId,
    vector: Vector,
    neighbor_ids: HashMap<LayerNum, Vec<NodeId>>,
}

impl Node {
    pub fn new(id: NodeId, vector: Vector) -> Self {
        Self {
            id,
            vector,
            neighbor_ids: HashMap::new(),
        }
    }

    pub fn vector(&self) -> &Vector {
        &self.vector
    }

    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn neighbor_ids(&self, layer: LayerNum) -> Option<&Vec<NodeId>> {
        self.neighbor_ids.get(&layer)
    }

    pub fn add_neighbor(&mut self, layer: LayerNum, neighbor_id: NodeId) {
        self.neighbor_ids
            .entry(layer)
            .or_insert_with(Vec::new)
            .push(neighbor_id);
    }

    pub fn set_neighbor_ids(&mut self, layer: LayerNum, neighbor_ids: Vec<NodeId>) {
        self.neighbor_ids.insert(layer, neighbor_ids);
    }
}

impl Hash for Node {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
}
