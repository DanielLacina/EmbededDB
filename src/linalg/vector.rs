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
        if self.data.len() != other.data.len() {
            panic!("Vectors must be of the same length to compute squared distance");
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_and_data() {
        let data = vec![1.0, 2.5, 3.0];
        let vector = Vector::new(data.clone());
        assert_eq!(vector.data(), &data, "The data getter should return the original vector data.");
    }

    #[test]
    fn test_squared_distance_correctness() {
        let vec1 = Vector::new(vec![1.0, 2.0, 3.0]);
        let vec2 = Vector::new(vec![4.0, 5.0, 6.0]);
        
        let expected_distance = 27.0;
        
        assert_eq!(vec1.squared_distance(&vec2), expected_distance);
    }
    
    #[test]
    fn test_squared_distance_to_self_is_zero() {
        let vec1 = Vector::new(vec![10.5, -5.0, 0.0]);
        let expected_distance = 0.0;

        assert_eq!(vec1.squared_distance(&vec1), expected_distance, "The distance to itself should be zero.");
    }
    
    #[test]
    #[should_panic(expected = "Vectors must be of the same length")]
    fn test_squared_distance_panics_on_mismatched_lengths() {
        let vec1 = Vector::new(vec![1.0, 2.0]);
        let vec2 = Vector::new(vec![1.0, 2.0, 3.0]);

        vec1.squared_distance(&vec2);
    }

    #[test]
    fn test_squared_distance_with_empty_vectors() {
        let vec1 = Vector::new(vec![]);
        let vec2 = Vector::new(vec![]);
        
        let expected_distance = 0.0;
        assert_eq!(vec1.squared_distance(&vec2), expected_distance, "The distance between two empty vectors should be zero.");
    }
}
