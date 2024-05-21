/// Configuration to control the mount/unmount behavior of the VFS.
pub struct Options {
	/// The mount options.
	pub mount: Mount,

	/// The unmount options.
	pub unmount: Mount,
}

pub struct Mount {
	/// The command to use to perform the mount/unmount.
	pub command: String,

	/// Command line arguments passed to the mount/unmount command.
	pub args: Vec<String>,
}
