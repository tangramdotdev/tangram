use std::ops::Range;

pub use self::{
	builder::Builder,
	data::Blob as Data,
	handle::Blob as Handle,
	id::Id,
	object::{Blob as Object, Child},
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;
pub mod read;

/// The average size of a blob leaf's bytes.
pub const AVG_LEAF_SIZE: usize = 65_536;

/// The maximum number of children of a blob branch.
pub const MAX_BRANCH_CHILDREN: usize = 1_024;

/// The maximum size of a blob leaf's bytes.
pub const MAX_LEAF_SIZE: usize = 131_072;

/// The minimum size of a blob leaf's bytes, except for the last leaf.
pub const MIN_LEAF_SIZE: usize = 4_096;

/// Get the ranges of the leaves that bytes are split into.
#[must_use]
pub fn chunks(bytes: &[u8]) -> Vec<Range<usize>> {
	fastcdc::v2020::FastCDC::new(bytes, MIN_LEAF_SIZE, AVG_LEAF_SIZE, MAX_LEAF_SIZE)
		.map(|chunk| chunk.offset..chunk.offset + chunk.length)
		.collect()
}
