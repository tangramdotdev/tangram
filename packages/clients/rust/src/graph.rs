pub use self::{
	builder::Builder,
	data::Graph as Data,
	handle::Graph as Handle,
	id::Id,
	object::{
		Branch, Child, Dependency, Directory, DirectoryBranch, DirectoryChild, DirectoryLeaf, Edge,
		File, Graph as Object, Leaf, Node, Pointer, Symlink,
	},
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// The maximum number of children of a graph branch.
pub const MAX_BRANCH_CHILDREN: usize = 1_024;
