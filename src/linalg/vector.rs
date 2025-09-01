use std::hash::{Hash, Hasher};
use std::iter::zip;

#[derive(Clone, Debug)]
pub struct Vector {
    data: Vec<f64>,
}

impl Vector {
    pub fn new(data: Vec<f64>) -> Self {
        Vector { data }
    }

    pub fn data(&self) -> &Vec<f64> {
        &self.data
    }

    pub fn squared_distance(&self, other: &Self) -> f64 {
        zip(self.data(), other.data())
            .map(|(cur, other)| (cur - other).powi(2))
            .sum::<f64>()
    }
}

impl PartialEq for Vector {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

impl Eq for Vector {}

impl Hash for Vector {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        for val in &self.data {
            let bits = val.to_bits();
            bits.hash(state);
        }
    }
}
