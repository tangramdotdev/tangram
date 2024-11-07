pub use self::{
	builder::Builder, data::File as Data, handle::File as Handle, id::Id, object::File as Object,
};

pub mod builder;
pub mod data;
pub mod handle;
pub mod id;
pub mod object;

/// The extended attribute name used to store file data.
pub const XATTR_DATA_NAME: &str = "user.tangram.data";
pub const XATTR_METADATA_NAME: &str = "user.tangram.metadata";
