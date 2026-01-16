use std::cmp::min;

use crate::common::error::GroupAssignerError;

pub struct Assigner {}

#[derive(Debug, Clone)]
pub struct Group<T> {
    pub values: Vec<T>,
}
impl Assigner {
    /// Assign values to groups using a round-robin strategy.
    ///
    /// # Arguments
    /// - `all_values`: The list of values to assign.
    /// - `group_count`: The number of groups to create.
    ///
    /// # Returns
    /// Ok(Vec<Group<T>>) if the assignment was successful.
    /// Err(Error) if the assignment failed.
    pub fn assign_round_robin<T: Clone>(
        all_values: &[T],
        group_count: usize,
    ) -> Result<Vec<Group<T>>, GroupAssignerError> {
        let group_count = min(all_values.len(), group_count.max(1));

        let mut groups: Vec<Group<T>> = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            groups.push(Group { values: Vec::new() });
        }

        for (i, value) in all_values.iter().enumerate() {
            let group_index = i % group_count;
            groups[group_index].values.push(value.clone());
        }

        Ok(groups)
    }
}
