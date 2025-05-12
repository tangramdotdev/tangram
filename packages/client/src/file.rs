pub use self::{
	builder::Builder,
	data::File as Data,
	handle::{File as Handle, State},
	id::Id,
	object::File as Object,
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// The extended attribute name used to store lockfile data.
pub const XATTR_LOCK_NAME: &str = "user.tangram.lock";
