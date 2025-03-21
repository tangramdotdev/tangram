#![allow(warnings)]

pub const FUSE_KERNEL_VERSION: u32 = 7;
pub const FUSE_KERNEL_MINOR_VERSION: u32 = 39;
pub const FUSE_ROOT_ID: u32 = 1;
pub const FATTR_MODE: u32 = 1;
pub const FATTR_UID: u32 = 2;
pub const FATTR_GID: u32 = 4;
pub const FATTR_SIZE: u32 = 8;
pub const FATTR_ATIME: u32 = 16;
pub const FATTR_MTIME: u32 = 32;
pub const FATTR_FH: u32 = 64;
pub const FATTR_ATIME_NOW: u32 = 128;
pub const FATTR_MTIME_NOW: u32 = 256;
pub const FATTR_LOCKOWNER: u32 = 512;
pub const FATTR_CTIME: u32 = 1024;
pub const FATTR_KILL_SUIDGID: u32 = 2048;
pub const FOPEN_DIRECT_IO: u32 = 1;
pub const FOPEN_KEEP_CACHE: u32 = 2;
pub const FOPEN_NONSEEKABLE: u32 = 4;
pub const FOPEN_CACHE_DIR: u32 = 8;
pub const FOPEN_STREAM: u32 = 16;
pub const FOPEN_NOFLUSH: u32 = 32;
pub const FOPEN_PARALLEL_DIRECT_WRITES: u32 = 64;
pub const FUSE_ASYNC_READ: u32 = 1;
pub const FUSE_POSIX_LOCKS: u32 = 2;
pub const FUSE_FILE_OPS: u32 = 4;
pub const FUSE_ATOMIC_O_TRUNC: u32 = 8;
pub const FUSE_EXPORT_SUPPORT: u32 = 16;
pub const FUSE_BIG_WRITES: u32 = 32;
pub const FUSE_DONT_MASK: u32 = 64;
pub const FUSE_SPLICE_WRITE: u32 = 128;
pub const FUSE_SPLICE_MOVE: u32 = 256;
pub const FUSE_SPLICE_READ: u32 = 512;
pub const FUSE_FLOCK_LOCKS: u32 = 1024;
pub const FUSE_HAS_IOCTL_DIR: u32 = 2048;
pub const FUSE_AUTO_INVAL_DATA: u32 = 4096;
pub const FUSE_DO_READDIRPLUS: u32 = 8192;
pub const FUSE_READDIRPLUS_AUTO: u32 = 16384;
pub const FUSE_ASYNC_DIO: u32 = 32768;
pub const FUSE_WRITEBACK_CACHE: u32 = 65536;
pub const FUSE_NO_OPEN_SUPPORT: u32 = 131072;
pub const FUSE_PARALLEL_DIROPS: u32 = 262144;
pub const FUSE_HANDLE_KILLPRIV: u32 = 524288;
pub const FUSE_POSIX_ACL: u32 = 1048576;
pub const FUSE_ABORT_ERROR: u32 = 2097152;
pub const FUSE_MAX_PAGES: u32 = 4194304;
pub const FUSE_CACHE_SYMLINKS: u32 = 8388608;
pub const FUSE_NO_OPENDIR_SUPPORT: u32 = 16777216;
pub const FUSE_EXPLICIT_INVAL_DATA: u32 = 33554432;
pub const FUSE_MAP_ALIGNMENT: u32 = 67108864;
pub const FUSE_SUBMOUNTS: u32 = 134217728;
pub const FUSE_HANDLE_KILLPRIV_V2: u32 = 268435456;
pub const FUSE_SETXATTR_EXT: u32 = 536870912;
pub const FUSE_INIT_EXT: u32 = 1073741824;
pub const FUSE_INIT_RESERVED: u32 = 2147483648;
pub const FUSE_SECURITY_CTX: u64 = 4294967296;
pub const FUSE_HAS_INODE_DAX: u64 = 8589934592;
pub const CUSE_UNRESTRICTED_IOCTL: u32 = 1;
pub const FUSE_RELEASE_FLUSH: u32 = 1;
pub const FUSE_RELEASE_FLOCK_UNLOCK: u32 = 2;
pub const FUSE_GETATTR_FH: u32 = 1;
pub const FUSE_LK_FLOCK: u32 = 1;
pub const FUSE_WRITE_CACHE: u32 = 1;
pub const FUSE_WRITE_LOCKOWNER: u32 = 2;
pub const FUSE_WRITE_KILL_SUIDGID: u32 = 4;
pub const FUSE_WRITE_KILL_PRIV: u32 = 4;
pub const FUSE_READ_LOCKOWNER: u32 = 2;
pub const FUSE_IOCTL_COMPAT: u32 = 1;
pub const FUSE_IOCTL_UNRESTRICTED: u32 = 2;
pub const FUSE_IOCTL_RETRY: u32 = 4;
pub const FUSE_IOCTL_32BIT: u32 = 8;
pub const FUSE_IOCTL_DIR: u32 = 16;
pub const FUSE_IOCTL_COMPAT_X32: u32 = 32;
pub const FUSE_IOCTL_MAX_IOV: u32 = 256;
pub const FUSE_POLL_SCHEDULE_NOTIFY: u32 = 1;
pub const FUSE_FSYNC_FDATASYNC: u32 = 1;
pub const FUSE_ATTR_SUBMOUNT: u32 = 1;
pub const FUSE_ATTR_DAX: u32 = 2;
pub const FUSE_OPEN_KILL_SUIDGID: u32 = 1;
pub const FUSE_SETXATTR_ACL_KILL_SGID: u32 = 1;
pub const FUSE_EXPIRE_ONLY: u32 = 1;
pub const FUSE_MIN_READ_BUFFER: u32 = 8192;
pub const FUSE_COMPAT_ENTRY_OUT_SIZE: u32 = 120;
pub const FUSE_COMPAT_ATTR_OUT_SIZE: u32 = 96;
pub const FUSE_COMPAT_MKNOD_IN_SIZE: u32 = 8;
pub const FUSE_COMPAT_WRITE_IN_SIZE: u32 = 24;
pub const FUSE_COMPAT_STATFS_SIZE: u32 = 48;
pub const FUSE_COMPAT_SETXATTR_IN_SIZE: u32 = 8;
pub const FUSE_COMPAT_INIT_OUT_SIZE: u32 = 8;
pub const FUSE_COMPAT_22_INIT_OUT_SIZE: u32 = 24;
pub const CUSE_INIT_INFO_MAX: u32 = 4096;
pub const FUSE_DEV_IOC_MAGIC: u32 = 229;
pub const FUSE_SETUPMAPPING_FLAG_WRITE: u32 = 1;
pub const FUSE_SETUPMAPPING_FLAG_READ: u32 = 2;

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_attr {
	pub ino: u64,
	pub size: u64,
	pub blocks: u64,
	pub atime: u64,
	pub mtime: u64,
	pub ctime: u64,
	pub atimensec: u32,
	pub mtimensec: u32,
	pub ctimensec: u32,
	pub mode: u32,
	pub nlink: u32,
	pub uid: u32,
	pub gid: u32,
	pub rdev: u32,
	pub blksize: u32,
	pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_sx_time {
	pub tv_sec: i64,
	pub tv_nsec: u32,
	pub __reserved: i32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_statx {
	pub mask: u32,
	pub blksize: u32,
	pub attributes: u64,
	pub nlink: u32,
	pub uid: u32,
	pub gid: u32,
	pub mode: u16,
	pub __spare0: [u16; 1],
	pub ino: u64,
	pub size: u64,
	pub blocks: u64,
	pub attributes_mask: u64,
	pub atime: fuse_sx_time,
	pub btime: fuse_sx_time,
	pub ctime: fuse_sx_time,
	pub mtime: fuse_sx_time,
	pub rdev_major: u32,
	pub rdev_minor: u32,
	pub dev_major: u32,
	pub dev_minor: u32,
	pub __spare2: [u64; 14],
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_kstatfs {
	pub blocks: u64,
	pub bfree: u64,
	pub bavail: u64,
	pub files: u64,
	pub ffree: u64,
	pub bsize: u32,
	pub namelen: u32,
	pub frsize: u32,
	pub padding: u32,
	pub spare: [u32; 6usize],
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_file_lock {
	pub start: u64,
	pub end: u64,
	pub type_: u32,
	pub pid: u32,
}

pub mod fuse_opcode {
	pub type Type = ::std::os::raw::c_uint;
	pub const FUSE_LOOKUP: Type = 1;
	pub const FUSE_FORGET: Type = 2;
	pub const FUSE_GETATTR: Type = 3;
	pub const FUSE_SETATTR: Type = 4;
	pub const FUSE_READLINK: Type = 5;
	pub const FUSE_SYMLINK: Type = 6;
	pub const FUSE_MKNOD: Type = 8;
	pub const FUSE_MKDIR: Type = 9;
	pub const FUSE_UNLINK: Type = 10;
	pub const FUSE_RMDIR: Type = 11;
	pub const FUSE_RENAME: Type = 12;
	pub const FUSE_LINK: Type = 13;
	pub const FUSE_OPEN: Type = 14;
	pub const FUSE_READ: Type = 15;
	pub const FUSE_WRITE: Type = 16;
	pub const FUSE_STATFS: Type = 17;
	pub const FUSE_RELEASE: Type = 18;
	pub const FUSE_FSYNC: Type = 20;
	pub const FUSE_SETXATTR: Type = 21;
	pub const FUSE_GETXATTR: Type = 22;
	pub const FUSE_LISTXATTR: Type = 23;
	pub const FUSE_REMOVEXATTR: Type = 24;
	pub const FUSE_FLUSH: Type = 25;
	pub const FUSE_INIT: Type = 26;
	pub const FUSE_OPENDIR: Type = 27;
	pub const FUSE_READDIR: Type = 28;
	pub const FUSE_RELEASEDIR: Type = 29;
	pub const FUSE_FSYNCDIR: Type = 30;
	pub const FUSE_GETLK: Type = 31;
	pub const FUSE_SETLK: Type = 32;
	pub const FUSE_SETLKW: Type = 33;
	pub const FUSE_ACCESS: Type = 34;
	pub const FUSE_CREATE: Type = 35;
	pub const FUSE_INTERRUPT: Type = 36;
	pub const FUSE_BMAP: Type = 37;
	pub const FUSE_DESTROY: Type = 38;
	pub const FUSE_IOCTL: Type = 39;
	pub const FUSE_POLL: Type = 40;
	pub const FUSE_NOTIFY_REPLY: Type = 41;
	pub const FUSE_BATCH_FORGET: Type = 42;
	pub const FUSE_FALLOCATE: Type = 43;
	pub const FUSE_READDIRPLUS: Type = 44;
	pub const FUSE_RENAME2: Type = 45;
	pub const FUSE_LSEEK: Type = 46;
	pub const FUSE_COPY_FILE_RANGE: Type = 47;
	pub const FUSE_SETUPMAPPING: Type = 48;
	pub const FUSE_REMOVEMAPPING: Type = 49;
	pub const FUSE_SYNCFS: Type = 50;
	pub const FUSE_TMPFILE: Type = 51;
	pub const FUSE_STATX: Type = 52;
	pub const CUSE_INIT: Type = 4096;
	pub const CUSE_INIT_BSWAP_RESERVED: Type = 1048576;
	pub const FUSE_INIT_BSWAP_RESERVED: Type = 436207616;
}

pub mod fuse_notify_code {
	pub type Type = ::std::os::raw::c_uint;
	pub const FUSE_NOTIFY_POLL: Type = 1;
	pub const FUSE_NOTIFY_INVAL_INODE: Type = 2;
	pub const FUSE_NOTIFY_INVAL_ENTRY: Type = 3;
	pub const FUSE_NOTIFY_STORE: Type = 4;
	pub const FUSE_NOTIFY_RETRIEVE: Type = 5;
	pub const FUSE_NOTIFY_DELETE: Type = 6;
	pub const FUSE_NOTIFY_CODE_MAX: Type = 7;
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_entry_out {
	pub nodeid: u64,
	pub generation: u64,
	pub entry_valid: u64,
	pub attr_valid: u64,
	pub entry_valid_nsec: u32,
	pub attr_valid_nsec: u32,
	pub attr: fuse_attr,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_forget_in {
	pub nlookup: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_forget_one {
	pub nodeid: u64,
	pub nlookup: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_batch_forget_in {
	pub count: u32,
	pub dummy: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_getattr_in {
	pub getattr_flags: u32,
	pub dummy: u32,
	pub fh: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_attr_out {
	pub attr_valid: u64,
	pub attr_valid_nsec: u32,
	pub dummy: u32,
	pub attr: fuse_attr,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_statx_in {
	pub getattr_flags: u32,
	pub reserved: u32,
	pub fh: u64,
	pub sx_flags: u32,
	pub sx_mask: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_statx_out {
	pub attr_valid: u64,
	pub attr_valid_nsec: u32,
	pub flags: u32,
	pub spare: [u64; 2],
	pub stat: fuse_statx,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_mknod_in {
	pub mode: u32,
	pub rdev: u32,
	pub umask: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_mkdir_in {
	pub mode: u32,
	pub umask: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_rename_in {
	pub newdir: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_rename2_in {
	pub newdir: u64,
	pub flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_link_in {
	pub oldnodeid: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_setattr_in {
	pub valid: u32,
	pub padding: u32,
	pub fh: u64,
	pub size: u64,
	pub lock_owner: u64,
	pub atime: u64,
	pub mtime: u64,
	pub ctime: u64,
	pub atimensec: u32,
	pub mtimensec: u32,
	pub ctimensec: u32,
	pub mode: u32,
	pub unused4: u32,
	pub uid: u32,
	pub gid: u32,
	pub unused5: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_open_in {
	pub flags: u32,
	pub open_flags: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_create_in {
	pub flags: u32,
	pub mode: u32,
	pub umask: u32,
	pub open_flags: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_open_out {
	pub fh: u64,
	pub open_flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_release_in {
	pub fh: u64,
	pub flags: u32,
	pub release_flags: u32,
	pub lock_owner: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_flush_in {
	pub fh: u64,
	pub unused: u32,
	pub padding: u32,
	pub lock_owner: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_read_in {
	pub fh: u64,
	pub offset: u64,
	pub size: u32,
	pub read_flags: u32,
	pub lock_owner: u64,
	pub flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_write_in {
	pub fh: u64,
	pub offset: u64,
	pub size: u32,
	pub write_flags: u32,
	pub lock_owner: u64,
	pub flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_write_out {
	pub size: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_statfs_out {
	pub st: fuse_kstatfs,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_fsync_in {
	pub fh: u64,
	pub fsync_flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_setxattr_in {
	pub size: u32,
	pub flags: u32,
	pub setxattr_flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_getxattr_in {
	pub size: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_getxattr_out {
	pub size: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_lk_in {
	pub fh: u64,
	pub owner: u64,
	pub lk: fuse_file_lock,
	pub lk_flags: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_lk_out {
	pub lk: fuse_file_lock,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_access_in {
	pub mask: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_init_in {
	pub major: u32,
	pub minor: u32,
	pub max_readahead: u32,
	pub flags: u32,
	pub flags2: u32,
	pub unused: [u32; 11usize],
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_init_out {
	pub major: u32,
	pub minor: u32,
	pub max_readahead: u32,
	pub flags: u32,
	pub max_background: u16,
	pub congestion_threshold: u16,
	pub max_write: u32,
	pub time_gran: u32,
	pub max_pages: u16,
	pub map_alignment: u16,
	pub flags2: u32,
	pub unused: [u32; 7usize],
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct cuse_init_in {
	pub major: u32,
	pub minor: u32,
	pub unused: u32,
	pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct cuse_init_out {
	pub major: u32,
	pub minor: u32,
	pub unused: u32,
	pub flags: u32,
	pub max_read: u32,
	pub max_write: u32,
	pub dev_major: u32,
	pub dev_minor: u32,
	pub spare: [u32; 10usize],
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_interrupt_in {
	pub unique: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_bmap_in {
	pub block: u64,
	pub blocksize: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_bmap_out {
	pub block: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_ioctl_in {
	pub fh: u64,
	pub flags: u32,
	pub cmd: u32,
	pub arg: u64,
	pub in_size: u32,
	pub out_size: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_ioctl_iovec {
	pub base: u64,
	pub len: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_ioctl_out {
	pub result: i32,
	pub flags: u32,
	pub in_iovs: u32,
	pub out_iovs: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_poll_in {
	pub fh: u64,
	pub kh: u64,
	pub flags: u32,
	pub events: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_poll_out {
	pub revents: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_poll_wakeup_out {
	pub kh: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_fallocate_in {
	pub fh: u64,
	pub offset: u64,
	pub length: u64,
	pub mode: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_in_header {
	pub len: u32,
	pub opcode: u32,
	pub unique: u64,
	pub nodeid: u64,
	pub uid: u32,
	pub gid: u32,
	pub pid: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_out_header {
	pub len: u32,
	pub error: i32,
	pub unique: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_dirent {
	pub ino: u64,
	pub off: u64,
	pub namelen: u32,
	pub type_: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_direntplus {
	pub entry_out: fuse_entry_out,
	pub dirent: fuse_dirent,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_inval_inode_out {
	pub ino: u64,
	pub off: i64,
	pub len: i64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_inval_entry_out {
	pub parent: u64,
	pub namelen: u32,
	pub flags: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_delete_out {
	pub parent: u64,
	pub child: u64,
	pub namelen: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_store_out {
	pub nodeid: u64,
	pub offset: u64,
	pub size: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_retrieve_out {
	pub notify_unique: u64,
	pub nodeid: u64,
	pub offset: u64,
	pub size: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_notify_retrieve_in {
	pub dummy1: u64,
	pub offset: u64,
	pub size: u32,
	pub dummy2: u32,
	pub dummy3: u64,
	pub dummy4: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_lseek_in {
	pub fh: u64,
	pub offset: u64,
	pub whence: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_lseek_out {
	pub offset: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_copy_file_range_in {
	pub fh_in: u64,
	pub off_in: u64,
	pub nodeid_out: u64,
	pub fh_out: u64,
	pub off_out: u64,
	pub len: u64,
	pub flags: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_setupmapping_in {
	pub fh: u64,
	pub foffset: u64,
	pub len: u64,
	pub flags: u64,
	pub moffset: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_removemapping_in {
	pub count: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_removemapping_one {
	pub moffset: u64,
	pub len: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_syncfs_in {
	pub padding: u64,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_secctx {
	pub size: u32,
	pub padding: u32,
}

#[repr(C)]
#[derive(Clone, Debug, zerocopy::FromBytes, zerocopy::Immutable, zerocopy::IntoBytes)]
pub struct fuse_secctx_header {
	pub size: u32,
	pub nr_secctx: u32,
}
