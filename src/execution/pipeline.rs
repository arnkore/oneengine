use crate::execution::operator::BoxedOperator;
use anyhow::Result;
use std::collections::HashMap;
use uuid::Uuid;

/// A pipeline of operators
pub struct Pipeline {
    pub id: Uuid,
    pub operators: Vec<BoxedOperator>,
    pub connections: HashMap<usize, Vec<usize>>, // operator_id -> downstream_operators
}

impl Pipeline {
    pub fn new() -> Self {
        Self {
            id: Uuid::new_v4(),
            operators: Vec::new(),
            connections: HashMap::new(),
        }
    }

    pub fn add_operator(&mut self, operator: BoxedOperator) -> usize {
        let id = self.operators.len();
        self.operators.push(operator);
        id
    }

    pub fn connect(&mut self, from: usize, to: usize) -> Result<()> {
        if from >= self.operators.len() || to >= self.operators.len() {
            return Err(anyhow::anyhow!("Invalid operator indices"));
        }
        self.connections.entry(from).or_insert_with(Vec::new).push(to);
        Ok(())
    }
}
