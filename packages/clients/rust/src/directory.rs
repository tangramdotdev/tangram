pub use self::{
	builder::Builder, data::Directory as Data, handle::Directory as Handle, id::Id,
	object::Directory as Object,
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// The maximum number of children of a directory branch.
pub const MAX_BRANCH_CHILDREN: usize = 128;

/// The maximum number of entries in a directory leaf.
pub const MAX_LEAF_ENTRIES: usize = 1_024;
