#![allow(non_camel_case_types, dead_code, clippy::match_same_arms)]

use super::xdr;
use num::ToPrimitive;

// RPC constants.
pub const RPC_VERS: u32 = 2;
pub const NFS_PROG: u32 = 100_003;
pub const NFS_VERS: u32 = 4;

pub type int32_t = i32;
pub type uint32_t = u32;
pub type int64_t = i64;
pub type uint64_t = u64;

pub const NFS4_FHSIZE: usize = 128;
pub const NFS4_VERIFIER_SIZE: usize = 8;
pub const NFS4_OTHER_SIZE: usize = 12;
pub const NFS4_OPAQUE_LIMIT: usize = 1024;

pub const NFS4_INT64_MAX: int64_t = 0x7fff_ffff_ffff_ffff;
pub const NFS4_UINT64_MAX: uint64_t = 0xffff_ffff_ffff_ffff;
pub const NFS4_INT32_MAX: int32_t = 0x7fff_ffff;
pub const NFS4_UINT32_MAX: uint32_t = 0xffff_ffff;

#[repr(i32)]
pub enum nfs_ftype4 {
	NF4REG = 1,       /* Regular File */
	NF4DIR = 2,       /* Directory */
	NF4BLK = 3,       /* Special File - block device */
	NF4CHR = 4,       /* Special File - character device */
	NF4LNK = 5,       /* Symbolic Link */
	NF4SOCK = 6,      /* Special File - socket */
	NF4FIFO = 7,      /* Special File - fifo */
	NF4ATTRDIR = 8,   /* Attribute Directory */
	NF4NAMEDATTR = 9, /* Named Attribute */
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum nfsstat4 {
	NFS4_OK = 0,         /* everything is okay       */
	NFS4ERR_PERM = 1,    /* caller not privileged    */
	NFS4ERR_NOENT = 2,   /* no such file/directory   */
	NFS4ERR_IO = 5,      /* hard I/O error           */
	NFS4ERR_NXIO = 6,    /* no such device           */
	NFS4ERR_ACCESS = 13, /* access denied            */
	NFS4ERR_EXIST = 17,  /* file already exists      */
	NFS4ERR_XDEV = 18,   /* different file systems   */
	/* Unused/reserved        19 */
	NFS4ERR_NOTDIR = 20,                 /* should be a directory    */
	NFS4ERR_ISDIR = 21,                  /* should not be directory  */
	NFS4ERR_INVAL = 22,                  /* invalid argument         */
	NFS4ERR_FBIG = 27,                   /* file exceeds server max  */
	NFS4ERR_NOSPC = 28,                  /* no space on file system  */
	NFS4ERR_ROFS = 30,                   /* read-only file system    */
	NFS4ERR_MLINK = 31,                  /* too many hard links      */
	NFS4ERR_NAMETOOLONG = 63,            /* name exceeds server max  */
	NFS4ERR_NOTEMPTY = 66,               /* directory not empty      */
	NFS4ERR_DQUOT = 69,                  /* hard quota limit reached */
	NFS4ERR_STALE = 70,                  /* file no longer exists    */
	NFS4ERR_BADHANDLE = 10001,           /* Illegal filehandle       */
	NFS4ERR_BAD_COOKIE = 10003,          /* READDIR cookie is stale  */
	NFS4ERR_NOTSUPP = 10004,             /* operation not supported  */
	NFS4ERR_TOOSMALL = 10005,            /* response limit exceeded  */
	NFS4ERR_SERVERFAULT = 10006,         /* undefined server error   */
	NFS4ERR_BADTYPE = 10007,             /* type invalid for CREATE  */
	NFS4ERR_DELAY = 10008,               /* file "busy" - retry      */
	NFS4ERR_SAME = 10009,                /* nverify says attrs same  */
	NFS4ERR_DENIED = 10010,              /* lock unavailable         */
	NFS4ERR_EXPIRED = 10011,             /* lock lease expired       */
	NFS4ERR_LOCKED = 10012,              /* I/O failed due to lock   */
	NFS4ERR_GRACE = 10013,               /* in grace period          */
	NFS4ERR_FHEXPIRED = 10014,           /* filehandle expired       */
	NFS4ERR_SHARE_DENIED = 10015,        /* share reserve denied     */
	NFS4ERR_WRONGSEC = 10016,            /* wrong security flavor    */
	NFS4ERR_CLID_INUSE = 10017,          /* clientid in use          */
	NFS4ERR_RESOURCE = 10018,            /* resource exhaustion      */
	NFS4ERR_MOVED = 10019,               /* file system relocated    */
	NFS4ERR_NOFILEHANDLE = 10020,        /* current FH is not set    */
	NFS4ERR_MINOR_VERS_MISMATCH = 10021, /* minor vers not supp */
	NFS4ERR_STALE_CLIENTID = 10022,      /* server has rebooted      */
	NFS4ERR_STALE_STATEID = 10023,       /* server has rebooted      */
	NFS4ERR_OLD_STATEID = 10024,         /* state is out of sync     */
	NFS4ERR_BAD_STATEID = 10025,         /* incorrect stateid        */
	NFS4ERR_BAD_SEQID = 10026,           /* request is out of seq.   */
	NFS4ERR_NOT_SAME = 10027,            /* verify - attrs not same  */
	NFS4ERR_LOCK_RANGE = 10028,          /* lock range not supported */
	NFS4ERR_SYMLINK = 10029,             /* should be file/directory */
	NFS4ERR_RESTOREFH = 10030,           /* no saved filehandle      */
	NFS4ERR_LEASE_MOVED = 10031,         /* some file system moved   */
	NFS4ERR_ATTRNOTSUPP = 10032,         /* recommended attr not sup */
	NFS4ERR_NO_GRACE = 10033,            /* reclaim outside of grace */
	NFS4ERR_RECLAIM_BAD = 10034,         /* reclaim error at server  */
	NFS4ERR_RECLAIM_CONFLICT = 10035,    /* conflict on reclaim    */
	NFS4ERR_BADXDR = 10036,              /* XDR decode failed        */
	NFS4ERR_LOCKS_HELD = 10037,          /* file locks held at CLOSE */
	NFS4ERR_OPENMODE = 10038,            /* conflict in OPEN and I/O */
	NFS4ERR_BADOWNER = 10039,            /* owner translation bad    */
	NFS4ERR_BADCHAR = 10040,             /* UTF-8 char not supported */
	NFS4ERR_BADNAME = 10041,             /* name not supported       */
	NFS4ERR_BAD_RANGE = 10042,           /* lock range not supported */
	NFS4ERR_LOCK_NOTSUPP = 10043,        /* no atomic up/downgrade   */
	NFS4ERR_OP_ILLEGAL = 10044,          /* undefined operation      */
	NFS4ERR_DEADLOCK = 10045,            /* file locking deadlock    */
	NFS4ERR_FILE_OPEN = 10046,           /* open file blocks op.     */
	NFS4ERR_ADMIN_REVOKED = 10047,       /* lock-owner state revoked */
	NFS4ERR_CB_PATH_DOWN = 10048,        /* callback path down       */
}

pub type attrlist4 = Vec<u8>;
#[derive(Clone)]
pub struct bitmap4(pub Vec<uint32_t>);
pub type changeid4 = uint64_t;
pub type clientid4 = uint64_t;
pub type count4 = uint32_t;
pub type length4 = uint64_t;
pub type mode4 = uint32_t;
pub type nfs_cookie4 = uint64_t;
/// Note: this is an opaque type that is left up to the server to define. We use 64 bit integers.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct nfs_fh4(pub u64);
pub type nfs_lease4 = uint32_t;
pub type offset4 = uint64_t;
pub type qop4 = uint32_t;
pub type sec_oid4 = Vec<u8>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct seqid4(uint32_t);
pub type utf8string = String;
pub type utf8str_cis = utf8string;
pub type utf8str_cs = utf8string;
pub type utf8str_mixed = utf8string;
pub type component4 = utf8str_cs;
pub type linktext4 = Vec<u8>;
pub type ascii_REQUIRED4 = utf8string;
#[derive(Clone, Debug)]
pub struct pathname4(pub Vec<component4>);
pub type nfs_lockid4 = uint64_t;
pub type verifier4 = [u8; NFS4_VERIFIER_SIZE];

/*
 * Timeval
 */
#[derive(Debug, Clone, Copy)]
pub struct nfstime4 {
	pub seconds: int64_t,
	pub nseconds: uint32_t,
}

#[repr(i32)]
pub enum time_how4 {
	SET_TO_SERVER_TIME4 = 0,
	SET_TO_CLIENT_TIME4 = 1,
}

#[derive(Debug, Clone, Copy)]
pub enum settime4 {
	SET_TO_SERVER_TIME4(nfstime4),
	SET_TO_CLIENT_TIME4,
}

/*
 * File attribute definitions
 */

/*
 * FSID structure for major/minor
 */

#[derive(Debug, Clone, Copy)]
pub struct fsid4 {
	pub major: uint64_t,
	pub minor: uint64_t,
}

/*
 * File system locations attribute for relocation/migration
 */

#[derive(Debug, Clone)]
pub struct fs_location4 {
	pub server: pathname4,
	pub rootpath: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct fs_locations4 {
	pub fs_root: pathname4,
	pub locations: Vec<fs_location4>,
}

/*
 * Various Access Control Entry definitions
 */

/*
 * Mask that indicates which Access Control Entries
 * are supported.  Values for the fattr4_aclsupport attribute.
 */
pub const ACL4_SUPPORT_ALLOW_ACL: u32 = 0x0000_0001;
pub const ACL4_SUPPORT_DENY_ACL: u32 = 0x0000_0002;
pub const ACL4_SUPPORT_AUDIT_ACL: u32 = 0x0000_0004;
pub const ACL4_SUPPORT_ALARM_ACL: u32 = 0x0000_0008;
pub type acetype4 = uint32_t;

/*
 * acetype4 values; others can be added as needed.
 */
pub const ACE4_ACCESS_ALLOWED_ACE_TYPE: acetype4 = 0x0000_0000;
pub const ACE4_ACCESS_DENIED_ACE_TYPE: acetype4 = 0x0000_0001;
pub const ACE4_SYSTEM_AUDIT_ACE_TYPE: acetype4 = 0x0000_0002;
pub const ACE4_SYSTEM_ALARM_ACE_TYPE: acetype4 = 0x0000_0003;

/*
 * ACE flag
 */
pub type aceflag4 = uint32_t;

/*
 * ACE flag values
 */
pub const ACE4_FILE_INHERIT_ACE: aceflag4 = 0x0000_0001;
pub const ACE4_DIRECTORY_INHERIT_ACE: aceflag4 = 0x0000_0002;
pub const ACE4_NO_PROPAGATE_INHERIT_ACE: aceflag4 = 0x0000_0004;
pub const ACE4_INHERIT_ONLY_ACE: aceflag4 = 0x0000_0008;
pub const ACE4_SUCCESSFUL_ACCESS_ACE_FLAG: aceflag4 = 0x0000_0010;
pub const ACE4_FAILED_ACCESS_ACE_FLAG: aceflag4 = 0x0000_0020;
pub const ACE4_IDENTIFIER_GROUP: aceflag4 = 0x0000_0040;

/*
 * ACE mask
 */
pub type acemask4 = uint32_t;

/*
 * ACE mask values
 */
pub const ACE4_READ_DATA: acemask4 = 0x0000_0001;
pub const ACE4_LIST_DIRECTORY: acemask4 = 0x0000_0001;
pub const ACE4_WRITE_DATA: acemask4 = 0x0000_0002;
pub const ACE4_ADD_FILE: acemask4 = 0x0000_0002;
pub const ACE4_APPEND_DATA: acemask4 = 0x0000_0004;
pub const ACE4_ADD_SUBDIRECTORY: acemask4 = 0x0000_0004;
pub const ACE4_READ_NAMED_ATTRS: acemask4 = 0x0000_0008;
pub const ACE4_WRITE_NAMED_ATTRS: acemask4 = 0x0000_0010;
pub const ACE4_EXECUTE: acemask4 = 0x0000_0020;
pub const ACE4_DELETE_CHILD: acemask4 = 0x0000_0040;
pub const ACE4_READ_ATTRIBUTES: acemask4 = 0x0000_0080;
pub const ACE4_WRITE_ATTRIBUTES: acemask4 = 0x0000_0100;

pub const ACE4_DELETE: u32 = 0x0001_0000;
pub const ACE4_READ_ACL: u32 = 0x0002_0000;
pub const ACE4_WRITE_ACL: u32 = 0x0004_0000;
pub const ACE4_WRITE_OWNER: u32 = 0x0008_0000;
pub const ACE4_SYNCHRONIZE: u32 = 0x0010_0000;

/*
 * ACE4_GENERIC_READ - defined as a combination of
 *      ACE4_READ_ACL |
 *      ACE4_READ_DATA |
 *      ACE4_READ_ATTRIBUTES |
 *      ACE4_SYNCHRONIZE
 */

pub const ACE4_GENERIC_READ: u32 = 0x0012_0081;

/*
 * ACE4_GENERIC_WRITE - defined as a combination of
 *      ACE4_READ_ACL |
 *      ACE4_WRITE_DATA |
 *      ACE4_WRITE_ATTRIBUTES |
 *      ACE4_WRITE_ACL |
 *      ACE4_APPEND_DATA |
 *      ACE4_SYNCHRONIZE
 */
pub const ACE4_GENERIC_WRITE: u32 = 0x0016_0106;
pub const ACE4_GENERIC_EXECUTE: u32 = 0x0012_00A0;

#[derive(Clone, Debug)]
pub struct nfsace4 {
	pub type_: acetype4,
	pub flag: aceflag4,
	pub access_mask: acemask4,
	pub who: utf8str_mixed,
}

/*
 * Field definitions for the fattr4_mode attribute
 */
pub const MODE4_SUID: fattr4_mode = 0x800; /* set user id on execution */
pub const MODE4_SGID: fattr4_mode = 0x400; /* set group id on execution */
pub const MODE4_SVTX: fattr4_mode = 0x200; /* save text even after use */
pub const MODE4_RUSR: fattr4_mode = 0x100; /* read permission: owner */
pub const MODE4_WUSR: fattr4_mode = 0x080; /* write permission: owner */
pub const MODE4_XUSR: fattr4_mode = 0x040; /* execute permission: owner */
pub const MODE4_RGRP: fattr4_mode = 0x020; /* read permission: group */
pub const MODE4_WGRP: fattr4_mode = 0x010; /* write permission: group */
pub const MODE4_XGRP: fattr4_mode = 0x008; /* execute permission: group */
pub const MODE4_ROTH: fattr4_mode = 0x004; /* read permission: other */
pub const MODE4_WOTH: fattr4_mode = 0x002; /* write permission: other */
pub const MODE4_XOTH: fattr4_mode = 0x001; /* execute permission: other */

/*
 * Special data/attribute associated with
 * file types NF4BLK and NF4CHR.
 */
#[derive(Clone, Copy, Debug)]
pub struct specdata4 {
	pub specdata1: uint32_t, /* major device number */
	pub specdata2: uint32_t, /* minor device number */
}

/*
 * Values for fattr4_fh_expire_type
 */
pub const FH4_PERSISTENT: fattr4_fh_expire_type = 0x0000_0000;
pub const FH4_NOEXPIRE_WITH_OPEN: fattr4_fh_expire_type = 0x0000_0001;
pub const FH4_VOLATILE_ANY: fattr4_fh_expire_type = 0x0000_0002;
pub const FH4_VOL_MIGRATION: fattr4_fh_expire_type = 0x0000_0004;
pub const FH4_VOL_RENAME: fattr4_fh_expire_type = 0x0000_0008;

pub type fattr4_supported_attrs = bitmap4;
pub type fattr4_type = nfs_ftype4;
pub type fattr4_fh_expire_type = uint32_t;
pub type fattr4_change = changeid4;
pub type fattr4_size = uint64_t;
pub type fattr4_link_support = bool;
pub type fattr4_symlink_support = bool;
pub type fattr4_named_attr = bool;
pub type fattr4_fsid = fsid4;

pub type fattr4_unique_handles = bool;
pub type fattr4_lease_time = nfs_lease4;
pub type fattr4_rdattr_error = nfsstat4;

pub type fattr4_acl = Vec<nfsace4>;
pub type fattr4_aclsupport = uint32_t;
pub type fattr4_archive = bool;
pub type fattr4_cansettime = bool;
pub type fattr4_case_insensitive = bool;
pub type fattr4_case_preserving = bool;
pub type fattr4_chown_restricted = bool;
pub type fattr4_fileid = uint64_t;
pub type fattr4_files_avail = uint64_t;
pub type fattr4_filehandle = nfs_fh4;
pub type fattr4_files_free = uint64_t;
pub type fattr4_files_total = uint64_t;
pub type fattr4_fs_locations = fs_locations4;
pub type fattr4_hidden = bool;
pub type fattr4_homogeneous = bool;
pub type fattr4_maxfilesize = uint64_t;
pub type fattr4_maxlink = uint32_t;
pub type fattr4_maxname = uint32_t;
pub type fattr4_maxread = uint64_t;
pub type fattr4_maxwrite = uint64_t;
pub type fattr4_mimetype = ascii_REQUIRED4;
pub type fattr4_mode = mode4;
pub type fattr4_mounted_on_fileid = uint64_t;
pub type fattr4_no_trunc = bool;
pub type fattr4_numlinks = uint32_t;
pub type fattr4_owner = utf8str_mixed;
pub type fattr4_owner_group = utf8str_mixed;
pub type fattr4_quota_avail_hard = uint64_t;
pub type fattr4_quota_avail_soft = uint64_t;
pub type fattr4_quota_used = uint64_t;
pub type fattr4_rawdev = specdata4;
pub type fattr4_space_avail = uint64_t;
pub type fattr4_space_free = uint64_t;
pub type fattr4_space_total = uint64_t;
pub type fattr4_space_used = uint64_t;
pub type fattr4_system = bool;
pub type fattr4_time_access = nfstime4;
pub type fattr4_time_access_set = settime4;
pub type fattr4_time_backup = nfstime4;
pub type fattr4_time_create = nfstime4;
pub type fattr4_time_delta = nfstime4;
pub type fattr4_time_metadata = nfstime4;
pub type fattr4_time_modify = nfstime4;
pub type fattr4_time_modify_set = settime4;

/*
 * Mandatory attributes
 */
pub const FATTR4_SUPPORTED_ATTRS: u32 = 0;
pub const FATTR4_TYPE: u32 = 1;
pub const FATTR4_FH_EXPIRE_TYPE: u32 = 2;
pub const FATTR4_CHANGE: u32 = 3;
pub const FATTR4_SIZE: u32 = 4;
pub const FATTR4_LINK_SUPPORT: u32 = 5;
pub const FATTR4_SYMLINK_SUPPORT: u32 = 6;
pub const FATTR4_NAMED_ATTR: u32 = 7;
pub const FATTR4_FSID: u32 = 8;
pub const FATTR4_UNIQUE_HANDLES: u32 = 9;
pub const FATTR4_LEASE_TIME: u32 = 10;
pub const FATTR4_RDATTR_ERROR: u32 = 11;
pub const FATTR4_FILEHANDLE: u32 = 19;

/*
 * Recommended attributes
 */
pub const FATTR4_ACL: u32 = 12;
pub const FATTR4_ACLSUPPORT: u32 = 13;
pub const FATTR4_ARCHIVE: u32 = 14;
pub const FATTR4_CANSETTIME: u32 = 15;
pub const FATTR4_CASE_INSENSITIVE: u32 = 16;
pub const FATTR4_CASE_PRESERVING: u32 = 17;
pub const FATTR4_CHOWN_RESTRICTED: u32 = 18;
pub const FATTR4_FILEID: u32 = 20;
pub const FATTR4_FILES_AVAIL: u32 = 21;
pub const FATTR4_FILES_FREE: u32 = 22;
pub const FATTR4_FILES_TOTAL: u32 = 23;
pub const FATTR4_FS_LOCATIONS: u32 = 24;
pub const FATTR4_HIDDEN: u32 = 25;
pub const FATTR4_HOMOGENEOUS: u32 = 26;
pub const FATTR4_MAXFILESIZE: u32 = 27;
pub const FATTR4_MAXLINK: u32 = 28;
pub const FATTR4_MAXNAME: u32 = 29;
pub const FATTR4_MAXREAD: u32 = 30;
pub const FATTR4_MAXWRITE: u32 = 31;
pub const FATTR4_MIMETYPE: u32 = 32;
pub const FATTR4_MODE: u32 = 33;
pub const FATTR4_NO_TRUNC: u32 = 34;
pub const FATTR4_NUMLINKS: u32 = 35;
pub const FATTR4_OWNER: u32 = 36;
pub const FATTR4_OWNER_GROUP: u32 = 37;
pub const FATTR4_QUOTA_AVAIL_HARD: u32 = 38;

pub const FATTR4_QUOTA_AVAIL_SOFT: u32 = 39;
pub const FATTR4_QUOTA_USED: u32 = 40;
pub const FATTR4_RAWDEV: u32 = 41;
pub const FATTR4_SPACE_AVAIL: u32 = 42;
pub const FATTR4_SPACE_FREE: u32 = 43;
pub const FATTR4_SPACE_TOTAL: u32 = 44;
pub const FATTR4_SPACE_USED: u32 = 45;
pub const FATTR4_SYSTEM: u32 = 46;
pub const FATTR4_TIME_ACCESS: u32 = 47;
pub const FATTR4_TIME_ACCESS_SET: u32 = 48;
pub const FATTR4_TIME_BACKUP: u32 = 49;
pub const FATTR4_TIME_CREATE: u32 = 50;
pub const FATTR4_TIME_DELTA: u32 = 51;
pub const FATTR4_TIME_METADATA: u32 = 52;
pub const FATTR4_TIME_MODIFY: u32 = 53;
pub const FATTR4_TIME_MODIFY_SET: u32 = 54;
pub const FATTR4_MOUNTED_ON_FILEID: u32 = 55;
pub const FATTR4_DIR_NOTIF_DELAY: u32 = 56;
pub const FATTR4_DIRENT_NOTIF_DELAY: u32 = 57;
pub const FATTR4_DACL: u32 = 58;
pub const FATTR4_SACL: u32 = 59;
pub const FATTR4_CHANGE_POLICY: u32 = 60;
pub const FATTR4_FS_STATUS: u32 = 61;
pub const FATTR4_FS_LAYOUT_TYPE: u32 = 62;
pub const FATTR4_LAYOUT_HINT: u32 = 63;
pub const FATTR4_LAYOUT_TYPE: u32 = 64;
pub const FATTR4_LAYOUT_BLKSIZE: u32 = 65;
pub const FATTR4_LAYOUT_ALIGNMENT: u32 = 66;
pub const FATTR4_FS_LOCATIONS_INFO: u32 = 67;
pub const FATTR4_MDSTHRESHOLD: u32 = 68;
pub const FATTR4_RETENTION_GET: u32 = 69;
pub const FATTR4_RETENTION_SET: u32 = 70;
pub const FATTR4_RETENTEVT_GET: u32 = 71;
pub const FATTR4_RETENTEVT_SET: u32 = 72;
pub const FATTR4_RETENTION_HOLD: u32 = 73;
pub const FATTR4_MODE_SET_MASKED: u32 = 74;
pub const FATTR4_SUPPATTR_EXCLCREAT: u32 = 75; // Required
pub const FATTR4_FS_CHARSET_CAP: u32 = 76;

/*
 * File attribute container
 */
#[derive(Clone, Debug)]
pub struct fattr4 {
	pub attrmask: bitmap4,
	pub attr_vals: attrlist4,
}

/*
 * Change info for the client
 */
#[derive(Clone, Copy, Debug)]
pub struct change_info4 {
	pub atomic: bool,
	pub before: changeid4,
	pub after: changeid4,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct clientaddr4 {
	/* see struct rpcb in RFC 1833 */
	pub r_netid: String, /* network id */
	pub r_addr: String,  /* universal address */
}

/*
 * Callback program info as provided by the client
 */
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct cb_client4 {
	pub cb_program: u32,
	pub cb_location: clientaddr4,
}

/*
 * Stateid
 */
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct stateid4 {
	pub seqid: seqid4,
	other: [u8; NFS4_OTHER_SIZE],
}

pub const ANONYMOUS_STATE_ID: stateid4 = stateid4 {
	seqid: seqid4(0),
	other: [0; NFS4_OTHER_SIZE],
};

pub const READ_BYPASS_STATE_ID: stateid4 = stateid4 {
	seqid: seqid4(u32::MAX),
	other: [0xff; NFS4_OTHER_SIZE],
};

/*
 * Client ID
 */
#[derive(Clone, Debug)]
pub struct nfs_client_id4 {
	pub verifier: verifier4,
	pub id: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct open_owner4 {
	pub clientid: clientid4,
	pub owner: Vec<u8>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct lock_owner4 {
	pub clientid: clientid4,
	pub owner: Vec<u8>,
}

#[repr(i32)]
#[derive(Clone, Copy, Debug)]
pub enum nfs_lock_type4 {
	READ_LT = 1,
	WRITE_LT = 2,
	READW_LT = 3,  /* blocking read */
	WRITEW_LT = 4, /* blocking write */
}

#[derive(Clone, Debug)]
pub struct open_to_lock_owner4 {
	pub open_seqid: seqid4,
	pub open_stateid: stateid4,
	pub lock_seqid: seqid4,
	pub lock_owner: lock_owner4,
}

/*
 * For LOCK, existing lock_owner continues to request file locks
 */
#[derive(Copy, Clone, Debug)]
pub struct exist_lock_owner4 {
	pub lock_stateid: stateid4,
	pub lock_seqid: seqid4,
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Clone, Debug)]
pub enum locker4 {
	TRUE(open_to_lock_owner4),
	FALSE(exist_lock_owner4),
}

pub const ACCESS4_READ: uint32_t = 0x0000_0001;
pub const ACCESS4_LOOKUP: uint32_t = 0x0000_0002;
pub const ACCESS4_MODIFY: uint32_t = 0x0000_0004;
pub const ACCESS4_EXTEND: uint32_t = 0x0000_0008;
pub const ACCESS4_DELETE: uint32_t = 0x0000_0010;
pub const ACCESS4_EXECUTE: uint32_t = 0x0000_0020;

#[derive(Clone, Copy, Debug)]
pub struct ACCESS4args {
	pub access: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct ACCESS4resok {
	pub supported: u32,
	pub access: u32,
}

#[derive(Clone, Copy, Debug)]
pub enum ACCESS4res {
	NFS4_OK(ACCESS4resok),
	Error(nfsstat4),
}

#[derive(Clone, Copy, Debug)]
pub struct CLOSE4args {
	pub seqid: seqid4,
	pub open_stateid: stateid4,
}

#[derive(Clone, Copy, Debug)]
pub enum CLOSE4res {
	NFS4_OK(stateid4),
	Error(nfsstat4),
}

#[derive(Clone, Debug)]
pub struct GETATTR4args {
	/* CURRENT_FH: directory or file */
	pub attr_request: bitmap4,
}

#[derive(Clone, Debug)]
pub struct GETATTR4resok {
	pub obj_attributes: fattr4,
}

#[derive(Clone, Debug)]
pub enum GETATTR4res {
	NFS4_OK(GETATTR4resok),
	Error(nfsstat4),
}

#[derive(Clone, Copy, Debug)]
pub enum GETFH4res {
	NFS4_OK(GETFH4resok),
	Error(nfsstat4),
}

#[derive(Clone, Copy, Debug)]
pub struct GETFH4resok {
	pub object: nfs_fh4,
}

#[derive(Clone, Debug)]
pub struct LOCK4args {
	/* CURRENT_FH: file */
	pub locktype: nfs_lock_type4,
	pub reclaim: bool,
	pub offset: offset4,
	pub length: length4,
	pub locker: locker4,
}

#[derive(Clone, Debug)]
pub struct LOCK4denied {
	pub offset: offset4,
	pub length: length4,
	pub locktype: nfs_lock_type4,
	pub owner: lock_owner4,
}

#[derive(Copy, Clone, Debug)]
pub struct LOCK4resok {
	pub lock_stateid: stateid4,
}

#[derive(Clone, Debug)]
pub enum LOCK4res {
	NFS4_OK(LOCK4resok),
	NFS4ERR_DENIED(LOCK4denied),
	Error(nfsstat4),
}

#[derive(Clone, Debug)]
pub struct LOCKT4args {
	/* CURRENT_FH: file */
	pub locktype: nfs_lock_type4,
	pub offset: offset4,
	pub length: length4,
	pub owner: lock_owner4,
}

#[derive(Clone, Debug)]
pub enum LOCKT4res {
	NFS4_OK,
	NFS4ERR_DENIED(LOCK4denied),
	Error(nfsstat4),
}

#[derive(Clone, Debug)]
pub struct LOCKU4args {
	pub locktype: nfs_lock_type4,
	pub seqid: seqid4,
	pub lock_stateid: stateid4,
	pub offset: offset4,
	pub length: length4,
}

#[derive(Clone, Debug)]
pub enum LOCKU4res {
	NFS4_OK(stateid4),
	Error(nfsstat4),
}

#[derive(Clone, Debug)]
pub struct LOOKUP4args {
	/* CURRENT_FH: directory */
	pub objname: component4,
}

#[derive(Clone, Copy, Debug)]
pub struct LOOKUP4res {
	/* CURRENT_FH: object */
	pub status: nfsstat4,
}

#[derive(Clone, Debug)]
pub struct LOOKUPP4res {
	pub status: nfsstat4,
}

#[derive(Clone, Debug)]
pub struct NVERIFY4args {
	pub obj_attributes: fattr4,
}

#[derive(Clone, Debug)]
pub struct NVERIFY4res {
	pub status: nfsstat4,
}

pub const OPEN4_SHARE_ACCESS_READ: u32 = 0x0000_0001;
pub const OPEN4_SHARE_ACCESS_WRITE: u32 = 0x0000_0002;
pub const OPEN4_SHARE_ACCESS_BOTH: u32 = 0x0000_0003;

pub const OPEN4_SHARE_DENY_NONE: u32 = 0x0000_0000;
pub const OPEN4_SHARE_DENY_READ: u32 = 0x0000_0001;
pub const OPEN4_SHARE_DENY_WRITE: u32 = 0x0000_0002;
pub const OPEN4_SHARE_DENY_BOTH: u32 = 0x0000_0003;
/*
 * Various definitions for OPEN
 */
#[repr(i32)]
#[derive(Clone, Copy, Debug)]
pub enum createmode4 {
	UNCHECKED4 = 0,
	GUARDED4 = 1,
	EXCLUSIVE4 = 2,
}

#[derive(Clone, Debug)]
pub enum createhow4 {
	UNCHECKED4(fattr4),
	GUARDED4(fattr4),
	EXCLUSIVE4(verifier4),
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum opentype4 {
	OPEN4_NOCREATE = 0,
	OPEN4_CREATE = 1,
}

#[derive(Clone, Debug)]
pub enum openflag4 {
	OPEN4_CREATE(createhow4),
	Default,
}

/* Next definitions used for OPEN delegation */
#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum limit_by4 {
	NFS_LIMIT_SIZE = 1,
	NFS_LIMIT_BLOCKS = 2, /* others as needed */
}

#[derive(Clone, Copy, Debug)]
pub struct nfs_modified_limit4 {
	num_blocks: uint32_t,
	bytes_per_block: uint32_t,
}

#[derive(Clone, Copy, Debug)]
pub enum nfs_space_limit4 {
	/* limit specified as file size */
	NFS_LIMIT_SIZE(uint64_t),
	/* limit specified by number of blocks */
	NFS_LIMIT_BLOCKS(nfs_modified_limit4),
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum open_delegation_type4 {
	OPEN_DELEGATE_NONE = 0,
	OPEN_DELEGATE_READ = 1,
	OPEN_DELEGATE_WRITE = 2,
}

#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum open_claim_type4 {
	CLAIM_NULL = 0,
	CLAIM_PREVIOUS = 1,
	CLAIM_DELEGATE_CUR = 2,
	CLAIM_DELEGATE_PREV = 3,
}

#[derive(Clone, Debug)]
pub struct open_claim_delegate_cur4 {
	pub delegate_stateid: stateid4,
	pub file: component4,
}

#[derive(Clone, Debug)]
pub enum open_claim4 {
	/*
		* No special rights to file.
		* Ordinary OPEN of the specified file.
		*/
	/* CURRENT_FH: directory */
	CLAIM_NULL(component4),

	/*
		* Right to the file established by an
		* open previous to server reboot.  File
		* identified by filehandle obtained at
		* that time rather than by name.
		*/
	/* CURRENT_FH: file being reclaimed */
	CLAIM_PREVIOUS(open_delegation_type4),

	/*
		* Right to file based on a delegation
		* granted by the server.  File is
		* specified by name.
		*/
	/* CURRENT_FH: directory */
	CLAIM_DELEGATE_CUR(open_claim_delegate_cur4),

	/*
		* Right to file based on a delegation
		* granted to a previous boot instance
		* of the client.  File is specified by name.
		*/
	/* CURRENT_FH: directory */
	CLAIM_DELEGATE_PREV(component4),
}

/*
 * OPEN: Open a file, potentially receiving an open delegation
 */
#[derive(Clone, Debug)]
pub struct OPEN4args {
	pub seqid: seqid4,
	pub share_access: uint32_t,
	pub share_deny: uint32_t,
	pub owner: open_owner4,
	pub openhow: openflag4,
	pub claim: open_claim4,
}

#[derive(Clone, Debug)]
pub struct open_read_delegation4 {
	/// Stateid for delegation
	pub stateid: stateid4,

	/// Pre-recalled flag for delegations obtainedby reclaim (CLAIM_PREVIOUS).
	pub recall: bool,

	/// Defines users who don't need an ACCESS call to open for read.
	pub permissions: nfsace4,
}

#[derive(Clone, Debug)]
pub struct open_write_delegation4 {
	/// Stateid for delegation
	pub stateid: stateid4,

	/// Pre-recalled flag for delegations obtained by reclaim (CLAIM_PREVIOUS).
	pub recall: bool,

	/// Defines condition that the client must check to determine whether the file needs to be flushed to the server on close.
	pub space_limit: nfs_space_limit4,

	/// Defines users who don't need an ACCESS call as part of a delegated open.
	pub permissions: nfsace4,
}

#[derive(Clone, Debug)]
pub enum open_delegation4 {
	OPEN_DELEGATE_NONE,
	OPEN_DELEGATE_READ(open_read_delegation4),
	OPEN_DELEGATE_WRITE(open_write_delegation4),
}

/*
 * Result flags
 */

/* Client must confirm open */
pub const OPEN4_RESULT_CONFIRM: u32 = 0x0000_0002;

/* Type of file locking behavior at the server */
pub const OPEN4_RESULT_LOCKTYPE_POSIX: u32 = 0x0000_0004;

#[derive(Clone, Debug)]
pub struct OPEN4resok {
	pub stateid: stateid4,   /* Stateid for open */
	pub cinfo: change_info4, /* Directory change info */
	pub rflags: uint32_t,    /* Result flags */
	pub attrset: bitmap4,    /* attribute set for create */
	pub delegation: open_delegation4, /* Info on any open
							 delegation */
}

#[derive(Clone, Debug)]
pub enum OPEN4res {
	/* CURRENT_FH: opened file */
	NFS4_OK(OPEN4resok),
	Error(nfsstat4),
}

#[derive(Copy, Clone, Debug)]
pub struct OPENATTR4args {
	pub createdir: bool,
}

#[derive(Copy, Clone, Debug)]
pub struct OPENATTR4res {
	pub status: nfsstat4,
}

#[derive(Copy, Clone, Debug)]
pub struct OPEN_CONFIRM4args {
	pub open_stateid: stateid4,
	pub seqid: seqid4,
}

#[derive(Copy, Clone, Debug)]
pub struct OPEN_CONFIRM4resok {
	pub open_stateid: stateid4,
}

#[derive(Copy, Clone, Debug)]
pub enum OPEN_CONFIRM4res {
	NFS4_OK(OPEN_CONFIRM4resok),
	Error(nfsstat4),
}

#[derive(Debug, Clone)]
pub struct PUTFH4args {
	pub object: nfs_fh4,
}

#[derive(Debug, Clone, Copy)]
pub struct PUTFH4res {
	/* CURRENT_FH */
	pub status: nfsstat4,
}

#[derive(Debug, Clone, Copy)]
pub struct PUTPUBFH4res {
	/* CURRENT_FH: */
	pub status: nfsstat4,
}

#[derive(Debug, Clone, Copy)]
pub struct PUTFPUBH4res {
	/* CURRENT_FH: pub fh */
	pub status: nfsstat4,
}

#[derive(Debug, Clone, Copy)]
pub struct PUTROOTFH4res {
	/* CURRENT_FH: root fh */
	pub status: nfsstat4,
}

#[derive(Debug, Clone, Copy)]
pub struct READ4args {
	/* CURRENT_FH: file */
	pub stateid: stateid4,
	pub offset: offset4,
	pub count: count4,
}

#[derive(Debug, Clone)]
pub struct READ4resok {
	pub eof: bool,
	pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub enum READ4res {
	NFS4_OK(READ4resok),
	Error(nfsstat4),
}

#[derive(Debug, Clone)]
pub struct READDIR4args {
	/* CURRENT_FH: directory */
	pub cookie: nfs_cookie4,
	pub cookieverf: verifier4,
	pub dircount: count4,
	pub maxcount: count4,
	pub attr_request: bitmap4,
}

#[derive(Debug, Clone)]
pub struct entry4 {
	pub cookie: nfs_cookie4,
	pub name: component4,
	pub attrs: fattr4,
}

#[derive(Debug, Clone)]
pub struct dirlist4 {
	pub entries: Vec<entry4>,
	pub eof: bool,
}

#[derive(Debug, Clone)]
pub struct READDIR4resok {
	pub cookieverf: verifier4,
	pub reply: dirlist4,
}

#[derive(Debug, Clone)]
pub enum READDIR4res {
	NFS4_OK(READDIR4resok),
	Error(nfsstat4),
}

#[derive(Debug, Clone)]
pub struct READLINK4resok {
	pub link: linktext4,
}

#[derive(Debug, Clone)]
pub enum READLINK4res {
	NFS4_OK(READLINK4resok),
	Error(nfsstat4),
}

#[derive(Clone, Copy, Debug)]
pub struct RENEW4args {
	pub clientid: clientid4,
}

#[derive(Clone, Copy, Debug)]
pub struct RENEW4res {
	pub status: nfsstat4,
}

#[derive(Clone, Copy, Debug)]
pub struct RESTOREFH4res {
	/* CURRENT_FH: value of saved fh */
	pub status: nfsstat4,
}

#[derive(Clone, Copy, Debug)]
pub struct SAVEFH4res {
	/* SAVED_FH: value of current fh */
	pub status: nfsstat4,
}

#[derive(Clone, Debug)]
pub struct SECINFO4args {
	/* CURRENT_FH: directory */
	pub name: component4,
}

/*
 * From RFC 2203
 */
#[derive(Clone, Copy, Debug)]
#[repr(i32)]
pub enum rpc_gss_svc_t {
	RPC_GSS_SVC_NONE = 1,
	RPC_GSS_SVC_INTEGRITY = 2,
	RPC_GSS_SVC_PRIVACY = 3,
}

#[derive(Clone, Debug)]
pub struct rpcsec_gss_info {
	pub oid: sec_oid4,
	pub qop: qop4,
	pub service: rpc_gss_svc_t,
}

/* RPCSEC_GSS has a value of '6'.  See RFC 2203 */
#[derive(Clone, Debug)]
pub enum secinfo4 {
	RPCSEC_GSS(rpcsec_gss_info),
	Default(u32),
}

pub type SECINFO4resok = Vec<secinfo4>;

#[derive(Clone, Debug)]
pub enum SECINFO4res {
	NFS4_OK(SECINFO4resok),
	Error(nfsstat4),
}

#[derive(Clone, Debug)]
pub struct SETCLIENTID4args {
	pub client: nfs_client_id4,
	pub callback: cb_client4,
	pub callback_ident: uint32_t,
}

#[derive(Clone, Debug)]
pub struct SETCLIENTID4resok {
	pub clientid: clientid4,
	pub setclientid_confirm: verifier4,
}

#[derive(Clone, Debug)]
pub enum SETCLIENTID4res {
	NFS4_OK(SETCLIENTID4resok),
	NFS4ERR_CLID_INUSE(clientaddr4),
	Error(nfsstat4),
}

#[derive(Clone, Copy, Debug)]
pub struct SETCLIENTID_CONFIRM4args {
	pub clientid: clientid4,
	pub setclientid_confirm: verifier4,
}

#[derive(Clone, Debug)]
pub struct RELEASE_LOCKOWNER4args {
	pub lock_owner: lock_owner4,
}

#[derive(Clone, Debug)]
pub struct RELEASE_LOCKOWNER4res {
	pub status: nfsstat4,
}

#[derive(Clone, Copy, Debug)]
pub struct SETCLIENTID_CONFIRM4res {
	pub status: nfsstat4,
}

#[derive(Clone, Copy, Debug)]
pub struct ILLEGAL4res {
	pub status: nfsstat4,
}

#[derive(Clone, Debug)]
pub struct COMPOUND4args {
	pub tag: utf8str_cs,
	pub minorversion: uint32_t,
	pub argarray: Vec<nfs_argop4>,
}

#[derive(Clone, Debug)]
pub struct COMPOUND4res {
	pub status: nfsstat4,
	pub tag: utf8str_cs,
	pub resarray: Vec<nfs_resop4>,
}

#[derive(Copy, Clone, Debug)]
#[repr(i32)]
pub enum nfs_opnum4 {
	OP_ACCESS = 3,
	OP_CLOSE = 4,
	OP_COMMIT = 5,
	OP_CREATE = 6,
	OP_DELEGPURGE = 7,
	OP_DELEGRETURN = 8,
	OP_GETATTR = 9,
	OP_GETFH = 10,
	OP_LINK = 11,
	OP_LOCK = 12,
	OP_LOCKT = 13,
	OP_LOCKU = 14,
	OP_LOOKUP = 15,
	OP_LOOKUPP = 16,
	OP_NVERIFY = 17,
	OP_OPEN = 18,
	OP_OPENATTR = 19,
	OP_OPEN_CONFIRM = 20,
	OP_OPEN_DOWNGRADE = 21,
	OP_PUTFH = 22,
	OP_PUTPUBFH = 23,
	OP_PUTROOTFH = 24,
	OP_READ = 25,
	OP_READDIR = 26,
	OP_READLINK = 27,
	OP_REMOVE = 28,
	OP_RENAME = 29,
	OP_RENEW = 30,
	OP_RESTOREFH = 31,
	OP_SAVEFH = 32,
	OP_SECINFO = 33,
	OP_SETATTR = 34,
	OP_SETCLIENTID = 35,
	OP_SETCLIENTID_CONFIRM = 36,
	OP_VERIFY = 37,
	OP_WRITE = 38,
	OP_RELEASE_LOCKOWNER = 39,
	OP_ILLEGAL = 10044,
}

#[derive(Clone, Debug)]
pub enum nfs_argop4 {
	OP_ACCESS(ACCESS4args),
	OP_CLOSE(CLOSE4args),
	OP_COMMIT,
	OP_CREATE,
	OP_DELEGPURGE,
	OP_DELEGRETURN,
	OP_GETATTR(GETATTR4args),
	OP_GETFH,
	OP_LINK,
	OP_LOCK(LOCK4args),
	OP_LOCKT(LOCKT4args),
	OP_LOCKU(LOCKU4args),
	OP_LOOKUP(LOOKUP4args),
	OP_LOOKUPP,
	OP_OPEN(OPEN4args),
	OP_NVERIFY(NVERIFY4args),
	OP_OPENATTR(OPENATTR4args),
	OP_OPEN_CONFIRM(OPEN_CONFIRM4args),
	OP_OPEN_DOWNGRADE,
	OP_PUTFH(PUTFH4args),
	OP_PUTPUBFH,
	OP_PUTROOTFH,
	OP_READ(READ4args),
	OP_READDIR(READDIR4args),
	OP_READLINK,
	OP_REMOVE,
	OP_RENAME,
	OP_RENEW(RENEW4args),
	OP_RESTOREFH,
	OP_SAVEFH,
	OP_SECINFO(SECINFO4args),
	OP_SETATTR,
	OP_SETCLIENTID(SETCLIENTID4args),
	OP_SETCLIENTID_CONFIRM(SETCLIENTID_CONFIRM4args),
	OP_VERIFY,
	OP_WRITE,
	OP_RELEASE_LOCKOWNER(RELEASE_LOCKOWNER4args),
	OP_ILLEGAL,
	Unimplemented(nfs_opnum4),
}

impl nfs_argop4 {
	pub fn name(&self) -> &'static str {
		match self {
			Self::OP_ACCESS(_) => "ACCESS",
			Self::OP_CLOSE(_) => "CLOSE",
			Self::OP_COMMIT => "COMMIT",
			Self::OP_CREATE => "CREATE",
			Self::OP_DELEGPURGE => "DELEGPURGE",
			Self::OP_DELEGRETURN => "DELEGRETURN",
			Self::OP_GETATTR(_) => "GETATTR",
			Self::OP_GETFH => "GETFH",
			Self::OP_LINK => "LINK",
			Self::OP_LOCK(_) => "LOCK",
			Self::OP_LOCKT(_) => "LOCKT",
			Self::OP_LOCKU(_) => "LOCKU",
			Self::OP_LOOKUP(_) => "LOOKUP",
			Self::OP_LOOKUPP => "LOOKUPP",
			Self::OP_OPEN(_) => "OPEN",
			Self::OP_NVERIFY(_) => "NVERIFY",
			Self::OP_OPENATTR(_) => "OPENATTR",
			Self::OP_OPEN_CONFIRM(_) => "OPEN_CONFIRM",
			Self::OP_OPEN_DOWNGRADE => "OPEN_DOWNGRADE",
			Self::OP_PUTFH(_) => "PUTFH",
			Self::OP_PUTPUBFH => "PUTPUBFH",
			Self::OP_PUTROOTFH => "PUTROOTFH",
			Self::OP_READ(_) => "READ",
			Self::OP_READDIR(_) => "READDIR",
			Self::OP_READLINK => "READLINK",
			Self::OP_REMOVE => "REMOVE",
			Self::OP_RENAME => "RENAME",
			Self::OP_RENEW(_) => "RENEW",
			Self::OP_RESTOREFH => "RESTOREFH",
			Self::OP_SAVEFH => "SAVEFH",
			Self::OP_SECINFO(_) => "SECINFO",
			Self::OP_SETATTR => "SETATTR",
			Self::OP_SETCLIENTID(_) => "SETCLIENTID",
			Self::OP_SETCLIENTID_CONFIRM(_) => "SETCLIENTID_CONFIRM",
			Self::OP_VERIFY => "VERIFY",
			Self::OP_WRITE => "WRITE",
			Self::OP_RELEASE_LOCKOWNER(_) => "RELEASE_LOCKOWNER",
			Self::OP_ILLEGAL => "ILLEGAL",
			Self::Unimplemented(_) => "Unimplemented",
		}
	}

	pub fn opnum(&self) -> nfs_opnum4 {
		match self {
			Self::OP_ACCESS(_) => nfs_opnum4::OP_ACCESS,
			Self::OP_CLOSE(_) => nfs_opnum4::OP_CLOSE,
			Self::OP_COMMIT => nfs_opnum4::OP_COMMIT,
			Self::OP_CREATE => nfs_opnum4::OP_CREATE,
			Self::OP_DELEGPURGE => nfs_opnum4::OP_DELEGPURGE,
			Self::OP_DELEGRETURN => nfs_opnum4::OP_DELEGRETURN,
			Self::OP_GETATTR(_) => nfs_opnum4::OP_GETATTR,
			Self::OP_GETFH => nfs_opnum4::OP_GETFH,
			Self::OP_LINK => nfs_opnum4::OP_LINK,
			Self::OP_LOCK(_) => nfs_opnum4::OP_LOCK,
			Self::OP_LOCKT(_) => nfs_opnum4::OP_LOCKT,
			Self::OP_LOCKU(_) => nfs_opnum4::OP_LOCKU,
			Self::OP_LOOKUP(_) => nfs_opnum4::OP_LOOKUP,
			Self::OP_LOOKUPP => nfs_opnum4::OP_LOOKUPP,
			Self::OP_OPEN(_) => nfs_opnum4::OP_OPEN,
			Self::OP_NVERIFY(_) => nfs_opnum4::OP_NVERIFY,
			Self::OP_OPENATTR(_) => nfs_opnum4::OP_OPENATTR,
			Self::OP_OPEN_CONFIRM(_) => nfs_opnum4::OP_OPEN_CONFIRM,
			Self::OP_OPEN_DOWNGRADE => nfs_opnum4::OP_OPEN_DOWNGRADE,
			Self::OP_PUTFH(_) => nfs_opnum4::OP_PUTFH,
			Self::OP_PUTPUBFH => nfs_opnum4::OP_PUTPUBFH,
			Self::OP_PUTROOTFH => nfs_opnum4::OP_PUTROOTFH,
			Self::OP_READ(_) => nfs_opnum4::OP_READ,
			Self::OP_READDIR(_) => nfs_opnum4::OP_READDIR,
			Self::OP_READLINK => nfs_opnum4::OP_READLINK,
			Self::OP_REMOVE => nfs_opnum4::OP_REMOVE,
			Self::OP_RENAME => nfs_opnum4::OP_RENAME,
			Self::OP_RENEW(_) => nfs_opnum4::OP_RENEW,
			Self::OP_RESTOREFH => nfs_opnum4::OP_RESTOREFH,
			Self::OP_SAVEFH => nfs_opnum4::OP_SAVEFH,
			Self::OP_SECINFO(_) => nfs_opnum4::OP_SECINFO,
			Self::OP_SETATTR => nfs_opnum4::OP_SETATTR,
			Self::OP_SETCLIENTID(_) => nfs_opnum4::OP_SETCLIENTID,
			Self::OP_SETCLIENTID_CONFIRM(_) => nfs_opnum4::OP_SETCLIENTID_CONFIRM,
			Self::OP_VERIFY => nfs_opnum4::OP_VERIFY,
			Self::OP_WRITE => nfs_opnum4::OP_WRITE,
			Self::OP_RELEASE_LOCKOWNER(_) => nfs_opnum4::OP_RELEASE_LOCKOWNER,
			Self::OP_ILLEGAL => nfs_opnum4::OP_ILLEGAL,
			Self::Unimplemented(op) => *op,
		}
	}
}
#[derive(Clone, Debug)]
pub enum nfs_resop4 {
	OP_ACCESS(ACCESS4res),
	OP_CLOSE(CLOSE4res),
	OP_COMMIT,
	OP_CREATE,
	OP_DELEGPURGE,
	OP_DELEGRETURN,
	OP_GETATTR(GETATTR4res),
	OP_GETFH(GETFH4res),
	OP_LINK,
	OP_LOCK(LOCK4res),
	OP_LOCKT(LOCKT4res),
	OP_LOCKU(LOCKU4res),
	OP_LOOKUP(LOOKUP4res),
	OP_LOOKUPP(LOOKUPP4res),
	OP_NVERIFY(NVERIFY4res),
	OP_OPEN(OPEN4res),
	OP_OPENATTR(OPENATTR4res),
	OP_OPEN_CONFIRM(OPEN_CONFIRM4res),
	OP_OPEN_DOWNGRADE,
	OP_PUTFH(PUTFH4res),
	OP_PUTPUBFH(PUTPUBFH4res),
	OP_PUTROOTFH(PUTROOTFH4res),
	OP_READ(READ4res),
	OP_READDIR(READDIR4res),
	OP_READLINK(READLINK4res),
	OP_REMOVE,
	OP_RENEW(RENEW4res),
	OP_RENAME,
	OP_RESTOREFH(RESTOREFH4res),
	OP_SAVEFH(SAVEFH4res),
	OP_SECINFO(SECINFO4res),
	OP_SETATTR,
	OP_SETCLIENTID(SETCLIENTID4res),
	OP_SETCLIENTID_CONFIRM(SETCLIENTID_CONFIRM4res),
	OP_VERIFY,
	OP_WRITE,
	OP_RELEASE_LOCKOWNER(RELEASE_LOCKOWNER4res),
	OP_ILLEGAL(ILLEGAL4res),
	Unknown(nfs_opnum4),
	Timeout(nfs_opnum4),
}

impl xdr::FromXdr for nfs_ftype4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		let ftype = match tag {
			1 => nfs_ftype4::NF4REG,
			2 => nfs_ftype4::NF4DIR,
			3 => nfs_ftype4::NF4BLK,
			4 => nfs_ftype4::NF4CHR,
			5 => nfs_ftype4::NF4LNK,
			6 => nfs_ftype4::NF4SOCK,
			7 => nfs_ftype4::NF4FIFO,
			8 => nfs_ftype4::NF4ATTRDIR,
			9 => nfs_ftype4::NF4NAMEDATTR,
			_ => return Err(xdr::Error::Custom("Expected a valid nfs_ftype4.".into())),
		};
		Ok(ftype)
	}
}

impl xdr::ToXdr for nfs_ftype4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			nfs_ftype4::NF4REG => encoder.encode_int(1)?,
			nfs_ftype4::NF4DIR => encoder.encode_int(2)?,
			nfs_ftype4::NF4BLK => encoder.encode_int(3)?,
			nfs_ftype4::NF4CHR => encoder.encode_int(4)?,
			nfs_ftype4::NF4LNK => encoder.encode_int(5)?,
			nfs_ftype4::NF4SOCK => encoder.encode_int(6)?,
			nfs_ftype4::NF4FIFO => encoder.encode_int(7)?,
			nfs_ftype4::NF4ATTRDIR => encoder.encode_int(8)?,
			nfs_ftype4::NF4NAMEDATTR => encoder.encode_int(9)?,
		}
		Ok(())
	}
}

impl xdr::ToXdr for nfsstat4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		let int = match self {
			nfsstat4::NFS4_OK => 0,
			nfsstat4::NFS4ERR_PERM => 1,
			nfsstat4::NFS4ERR_NOENT => 2,
			nfsstat4::NFS4ERR_IO => 5,
			nfsstat4::NFS4ERR_NXIO => 6,
			nfsstat4::NFS4ERR_ACCESS => 13,
			nfsstat4::NFS4ERR_EXIST => 17,
			nfsstat4::NFS4ERR_XDEV => 18,
			nfsstat4::NFS4ERR_NOTDIR => 20,
			nfsstat4::NFS4ERR_ISDIR => 21,
			nfsstat4::NFS4ERR_INVAL => 22,
			nfsstat4::NFS4ERR_FBIG => 27,
			nfsstat4::NFS4ERR_NOSPC => 28,
			nfsstat4::NFS4ERR_ROFS => 30,
			nfsstat4::NFS4ERR_MLINK => 31,
			nfsstat4::NFS4ERR_NAMETOOLONG => 63,
			nfsstat4::NFS4ERR_NOTEMPTY => 66,
			nfsstat4::NFS4ERR_DQUOT => 69,
			nfsstat4::NFS4ERR_STALE => 70,
			nfsstat4::NFS4ERR_BADHANDLE => 10001,
			nfsstat4::NFS4ERR_BAD_COOKIE => 10003,
			nfsstat4::NFS4ERR_NOTSUPP => 10004,
			nfsstat4::NFS4ERR_TOOSMALL => 10005,
			nfsstat4::NFS4ERR_SERVERFAULT => 10006,
			nfsstat4::NFS4ERR_BADTYPE => 10007,
			nfsstat4::NFS4ERR_DELAY => 10008,
			nfsstat4::NFS4ERR_SAME => 10009,
			nfsstat4::NFS4ERR_DENIED => 10010,
			nfsstat4::NFS4ERR_EXPIRED => 10011,
			nfsstat4::NFS4ERR_LOCKED => 10012,
			nfsstat4::NFS4ERR_GRACE => 10013,
			nfsstat4::NFS4ERR_FHEXPIRED => 10014,
			nfsstat4::NFS4ERR_SHARE_DENIED => 10015,
			nfsstat4::NFS4ERR_WRONGSEC => 10016,
			nfsstat4::NFS4ERR_CLID_INUSE => 10017,
			nfsstat4::NFS4ERR_RESOURCE => 10018,
			nfsstat4::NFS4ERR_MOVED => 10019,
			nfsstat4::NFS4ERR_NOFILEHANDLE => 10020,
			nfsstat4::NFS4ERR_MINOR_VERS_MISMATCH => 10021,
			nfsstat4::NFS4ERR_STALE_CLIENTID => 10022,
			nfsstat4::NFS4ERR_STALE_STATEID => 10023,
			nfsstat4::NFS4ERR_OLD_STATEID => 10024,
			nfsstat4::NFS4ERR_BAD_STATEID => 10025,
			nfsstat4::NFS4ERR_BAD_SEQID => 10026,
			nfsstat4::NFS4ERR_NOT_SAME => 10027,
			nfsstat4::NFS4ERR_LOCK_RANGE => 10028,
			nfsstat4::NFS4ERR_SYMLINK => 10029,
			nfsstat4::NFS4ERR_RESTOREFH => 10030,
			nfsstat4::NFS4ERR_LEASE_MOVED => 10031,
			nfsstat4::NFS4ERR_ATTRNOTSUPP => 10032,
			nfsstat4::NFS4ERR_NO_GRACE => 10033,
			nfsstat4::NFS4ERR_RECLAIM_BAD => 10034,
			nfsstat4::NFS4ERR_RECLAIM_CONFLICT => 10035,
			nfsstat4::NFS4ERR_BADXDR => 10036,
			nfsstat4::NFS4ERR_LOCKS_HELD => 10037,
			nfsstat4::NFS4ERR_OPENMODE => 10038,
			nfsstat4::NFS4ERR_BADOWNER => 10039,
			nfsstat4::NFS4ERR_BADCHAR => 10040,
			nfsstat4::NFS4ERR_BADNAME => 10041,
			nfsstat4::NFS4ERR_BAD_RANGE => 10042,
			nfsstat4::NFS4ERR_LOCK_NOTSUPP => 10043,
			nfsstat4::NFS4ERR_OP_ILLEGAL => 10044,
			nfsstat4::NFS4ERR_DEADLOCK => 10045,
			nfsstat4::NFS4ERR_FILE_OPEN => 10046,
			nfsstat4::NFS4ERR_ADMIN_REVOKED => 10047,
			nfsstat4::NFS4ERR_CB_PATH_DOWN => 10048,
		};
		encoder.encode_int(int)
	}
}

impl xdr::ToXdr for nfs_fh4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode_opaque(&self.0.to_be_bytes())?;
		Ok(())
	}
}

impl xdr::FromXdr for nfs_fh4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let bytes = decoder.decode_opaque()?;
		if bytes.len() != 8 {
			return Err(xdr::Error::Custom("File handle size mismatch.".into()));
		}
		let fh = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
		Ok(Self(fh))
	}
}

impl xdr::FromXdr for stateid4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let seqid = decoder.decode()?;
		let other = decoder.decode_n()?;
		Ok(Self { seqid, other })
	}
}

impl stateid4 {
	pub fn new(seqid: seqid4, index: u64, is_lock_set: bool) -> Self {
		let is_lock_set = u32::from(is_lock_set);
		let mut other = [0; NFS4_OTHER_SIZE];

		other[0..8].copy_from_slice(&index.to_be_bytes());
		other[8..12].copy_from_slice(&is_lock_set.to_be_bytes());
		Self { seqid, other }
	}

	pub fn index(&self) -> u64 {
		u64::from_be_bytes(self.other[0..8].try_into().unwrap())
	}

	pub fn is_lock_set(&self) -> bool {
		u32::from_be_bytes(self.other[8..12].try_into().unwrap()) == 1
	}

	pub fn is_valid(&self) -> bool {
		if [0, u64::MAX].contains(&self.index()) {
			[0, u32::MAX].contains(&self.seqid.0)
		} else {
			true
		}
	}
}

impl seqid4 {
	pub fn increment(self) -> Self {
		if self.0 == u32::MAX {
			Self(1)
		} else {
			Self(self.0 + 1)
		}
	}
}

impl xdr::ToXdr for seqid4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.0)
	}
}

impl xdr::FromXdr for seqid4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		Ok(Self(decoder.decode()?))
	}
}

impl xdr::ToXdr for change_info4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.atomic)?;
		encoder.encode(&self.before)?;
		encoder.encode(&self.after)?;
		Ok(())
	}
}

impl xdr::ToXdr for stateid4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.seqid)?;
		encoder.encode_n(self.other)?;
		Ok(())
	}
}

impl xdr::ToXdr for open_delegation4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::OPEN_DELEGATE_NONE => encoder.encode_int(0),
			_ => unimplemented!(),
		}
	}
}

impl xdr::ToXdr for fattr4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.attrmask.0)?;
		encoder.encode_opaque(&self.attr_vals)?;
		Ok(())
	}
}

impl xdr::FromXdr for fattr4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let attrmask = bitmap4(decoder.decode()?);
		let attr_vals = decoder.decode_opaque()?.to_owned();
		Ok(Self {
			attrmask,
			attr_vals,
		})
	}
}

impl xdr::ToXdr for cb_client4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode_uint(self.cb_program)?;
		encoder.encode(&self.cb_location)?;
		Ok(())
	}
}

impl xdr::FromXdr for cb_client4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let cb_program = decoder.decode_uint()?;
		let cb_location = decoder.decode()?;
		Ok(Self {
			cb_program,
			cb_location,
		})
	}
}

impl xdr::ToXdr for clientaddr4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode_str(&self.r_netid)?;
		encoder.encode_str(&self.r_addr)?;
		Ok(())
	}
}

impl xdr::FromXdr for clientaddr4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let r_netid = decoder.decode_str()?.to_owned();
		let r_addr = decoder.decode_str()?.to_owned();
		Ok(Self { r_netid, r_addr })
	}
}

impl xdr::ToXdr for fsid4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.major)?;
		encoder.encode(&self.minor)?;
		Ok(())
	}
}

impl xdr::ToXdr for nfsace4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.type_)?;
		encoder.encode(&self.flag)?;
		encoder.encode(&self.access_mask)?;
		encoder.encode(&self.who)?;
		Ok(())
	}
}

impl xdr::ToXdr for fs_locations4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.fs_root.0)?;
		encoder.encode(&self.locations)?;
		Ok(())
	}
}

impl xdr::ToXdr for fs_location4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.server.0)?;
		encoder.encode(&self.rootpath)?;
		Ok(())
	}
}

impl xdr::ToXdr for specdata4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.specdata1)?;
		encoder.encode(&self.specdata2)?;
		Ok(())
	}
}

impl xdr::ToXdr for nfstime4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.seconds)?;
		encoder.encode(&self.nseconds)?;
		Ok(())
	}
}

impl xdr::FromXdr for nfstime4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let seconds = decoder.decode()?;
		let nseconds = decoder.decode()?;
		Ok(Self { seconds, nseconds })
	}
}

impl xdr::FromXdr for open_owner4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let clientid = decoder.decode()?;
		let len = decoder.decode_uint()?;
		let bytes = decoder.decode_bytes(len.to_usize().unwrap())?;
		let owner = bytes.to_owned();
		Ok(Self { clientid, owner })
	}
}

impl xdr::FromXdr for createhow4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let type_ = decoder.decode_int()?;
		match type_ {
			0 => Ok(Self::UNCHECKED4(decoder.decode()?)),
			1 => Ok(Self::GUARDED4(decoder.decode()?)),
			2 => Ok(Self::EXCLUSIVE4(decoder.decode_n()?)),
			_ => Err(xdr::Error::Custom(
				"Expected a valid createhow4 value.".into(),
			)),
		}
	}
}

impl xdr::FromXdr for openflag4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let type_ = decoder.decode_int()?;
		match type_ {
			0 => Ok(Self::Default),
			1 => Ok(Self::OPEN4_CREATE(decoder.decode()?)),
			_ => Err(xdr::Error::Custom("Expected a valid opentype4.".into())),
		}
	}
}

impl xdr::FromXdr for open_claim4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		match tag {
			0 => Ok(Self::CLAIM_NULL(decoder.decode()?)),
			1 => Ok(Self::CLAIM_DELEGATE_PREV(decoder.decode()?)),
			2 => Ok(Self::CLAIM_DELEGATE_CUR(decoder.decode()?)),
			3 => Ok(Self::CLAIM_PREVIOUS(decoder.decode()?)),
			_ => Err(xdr::Error::Custom(
				"Expected a claim delegation type.".into(),
			)),
		}
	}
}

impl xdr::FromXdr for open_delegation_type4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_int()?;
		match tag {
			0 => Ok(Self::OPEN_DELEGATE_NONE),
			1 => Ok(Self::OPEN_DELEGATE_READ),
			2 => Ok(Self::OPEN_DELEGATE_WRITE),
			_ => Err(xdr::Error::Custom(
				"Expected an open delegation type.".into(),
			)),
		}
	}
}

impl xdr::FromXdr for open_claim_delegate_cur4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let delegate_stateid = decoder.decode()?;
		let file = decoder.decode()?;
		Ok(Self {
			delegate_stateid,
			file,
		})
	}
}

impl xdr::FromXdr for ACCESS4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let access = decoder.decode_uint()?;
		Ok(Self { access })
	}
}

impl xdr::ToXdr for ACCESS4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resop) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resop.supported)?;
				encoder.encode(&resop.access)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for CLOSE4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let seqid = decoder.decode()?;
		let open_stateid = decoder.decode()?;
		Ok(Self {
			seqid,
			open_stateid,
		})
	}
}

impl xdr::ToXdr for CLOSE4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			CLOSE4res::NFS4_OK(resop) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(resop)?;
			},
			CLOSE4res::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for GETATTR4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let attr_request = bitmap4(decoder.decode()?);
		Ok(Self { attr_request })
	}
}

impl xdr::ToXdr for GETATTR4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resok.obj_attributes)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::ToXdr for GETFH4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resok.object)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for PUTFH4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let object = decoder.decode()?;
		Ok(Self { object })
	}
}

impl xdr::ToXdr for PUTFH4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for PUTPUBFH4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for PUTROOTFH4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::FromXdr for nfs_lock_type4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		match decoder.decode_int()? {
			1 => Ok(nfs_lock_type4::READ_LT),
			2 => Ok(nfs_lock_type4::WRITE_LT),
			3 => Ok(nfs_lock_type4::READW_LT),
			4 => Ok(nfs_lock_type4::WRITEW_LT),
			_ => Err(xdr::Error::Custom(
				"Expected a valid nfs_lock_type4.".into(),
			)),
		}
	}
}

impl xdr::ToXdr for nfs_lock_type4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		let int = match self {
			nfs_lock_type4::READ_LT => 1,
			nfs_lock_type4::WRITE_LT => 2,
			nfs_lock_type4::READW_LT => 3,
			nfs_lock_type4::WRITEW_LT => 4,
		};
		encoder.encode_int(int)?;
		Ok(())
	}
}

impl xdr::FromXdr for locker4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		if decoder.decode_bool()? {
			let open_seqid = decoder.decode()?;
			let open_stateid = decoder.decode()?;
			let lock_seqid = decoder.decode()?;
			let lock_owner = decoder.decode()?;
			let locker = open_to_lock_owner4 {
				open_seqid,
				open_stateid,
				lock_seqid,
				lock_owner,
			};
			Ok(locker4::TRUE(locker))
		} else {
			let lock_stateid = decoder.decode()?;
			let lock_seqid = decoder.decode()?;
			let locker = exist_lock_owner4 {
				lock_stateid,
				lock_seqid,
			};
			Ok(locker4::FALSE(locker))
		}
	}
}

impl xdr::FromXdr for lock_owner4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let clientid = decoder.decode()?;
		let owner = decoder.decode_opaque()?.into();
		Ok(Self { clientid, owner })
	}
}

impl xdr::ToXdr for lock_owner4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.clientid)?;
		encoder.encode_opaque(&self.owner)?;
		Ok(())
	}
}

impl xdr::FromXdr for LOCK4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let locktype = decoder.decode()?;
		let reclaim = decoder.decode()?;
		let offset = decoder.decode()?;
		let length = decoder.decode()?;
		let locker = decoder.decode()?;
		Ok(Self {
			locktype,
			reclaim,
			offset,
			length,
			locker,
		})
	}
}

impl xdr::ToXdr for LOCK4denied {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.offset)?;
		encoder.encode(&self.length)?;
		encoder.encode(&self.locktype)?;
		encoder.encode(&self.owner)?;
		Ok(())
	}
}

impl xdr::ToXdr for LOCK4resok {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.lock_stateid)
	}
}

impl xdr::ToXdr for LOCK4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			LOCK4res::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(resok)?;
			},
			LOCK4res::NFS4ERR_DENIED(denied) => {
				encoder.encode(&nfsstat4::NFS4ERR_DENIED)?;
				encoder.encode(denied)?;
			},
			LOCK4res::Error(e) => encoder.encode(e)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for LOCKT4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let locktype = decoder.decode()?;
		let offset = decoder.decode()?;
		let length = decoder.decode()?;
		let owner = decoder.decode()?;
		Ok(Self {
			locktype,
			offset,
			length,
			owner,
		})
	}
}

impl xdr::ToXdr for LOCKT4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			LOCKT4res::NFS4_OK => encoder.encode(&nfsstat4::NFS4_OK)?,
			LOCKT4res::NFS4ERR_DENIED(res) => {
				encoder.encode(&nfsstat4::NFS4ERR_DENIED)?;
				encoder.encode(res)?;
			},
			LOCKT4res::Error(e) => encoder.encode(e)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for LOCKU4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let locktype = decoder.decode()?;
		let seqid = decoder.decode()?;
		let lock_stateid = decoder.decode()?;
		let offset = decoder.decode()?;
		let length = decoder.decode()?;
		Ok(Self {
			locktype,
			seqid,
			lock_stateid,
			offset,
			length,
		})
	}
}

impl xdr::ToXdr for LOCKU4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			LOCKU4res::NFS4_OK(lock_stateid) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(lock_stateid)?;
			},
			LOCKU4res::Error(e) => {
				encoder.encode(e)?;
			},
		}
		Ok(())
	}
}

impl xdr::FromXdr for LOOKUP4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let objname = decoder.decode()?;
		Ok(Self { objname })
	}
}

impl xdr::ToXdr for LOOKUP4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::FromXdr for NVERIFY4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let obj_attributes = decoder.decode()?;
		Ok(Self { obj_attributes })
	}
}

impl xdr::ToXdr for NVERIFY4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for LOOKUPP4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::FromXdr for OPEN4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let seqid = decoder.decode()?;
		let share_access = decoder.decode()?;
		let share_deny = decoder.decode()?;
		let owner = decoder.decode()?;
		let openhow = decoder.decode()?;
		let claim = decoder.decode()?;
		Ok(Self {
			seqid,
			share_access,
			share_deny,
			owner,
			openhow,
			claim,
		})
	}
}

impl xdr::ToXdr for OPEN4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			OPEN4res::NFS4_OK(resop) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resop.stateid)?;
				encoder.encode(&resop.cinfo)?;
				encoder.encode(&resop.rflags)?;
				encoder.encode(&resop.attrset.0)?;
				encoder.encode(&resop.delegation)?;
			},
			OPEN4res::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for OPENATTR4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let createdir = decoder.decode()?;
		Ok(Self { createdir })
	}
}

impl xdr::ToXdr for OPENATTR4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::FromXdr for OPEN_CONFIRM4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let open_stateid = decoder.decode()?;
		let seqid = decoder.decode()?;
		Ok(Self {
			open_stateid,
			seqid,
		})
	}
}

impl xdr::ToXdr for OPEN_CONFIRM4resok {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.open_stateid)
	}
}

impl xdr::ToXdr for OPEN_CONFIRM4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			OPEN_CONFIRM4res::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(resok)?;
			},
			OPEN_CONFIRM4res::Error(e) => encoder.encode(e)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for READ4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let stateid = decoder.decode()?;
		let offset = decoder.decode()?;
		let count = decoder.decode()?;
		Ok(Self {
			stateid,
			offset,
			count,
		})
	}
}

impl xdr::ToXdr for READ4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resop) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resop.eof)?;
				encoder.encode(&resop.data)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		};
		Ok(())
	}
}

impl xdr::FromXdr for READDIR4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let cookie = decoder.decode()?;
		let cookieverf = decoder.decode_n()?;
		let dircount = decoder.decode()?;
		let maxcount = decoder.decode()?;
		let attr_request = bitmap4(decoder.decode()?);
		Ok(Self {
			cookie,
			cookieverf,
			dircount,
			maxcount,
			attr_request,
		})
	}
}

impl xdr::ToXdr for READDIR4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			READDIR4res::NFS4_OK(resop) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode_n(resop.cookieverf)?;
				for entry in &resop.reply.entries {
					encoder.encode_bool(true)?;
					encoder.encode(entry)?;
				}
				encoder.encode_bool(false)?;
				encoder.encode_bool(resop.reply.eof)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::ToXdr for READLINK4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resok.link)?;
			},
			Self::Error(error) => encoder.encode(&error)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for RENEW4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let clientid = decoder.decode()?;
		Ok(Self { clientid })
	}
}

impl xdr::ToXdr for RENEW4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for RESTOREFH4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for SAVEFH4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::FromXdr for SECINFO4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let name = decoder.decode()?;
		Ok(Self { name })
	}
}

impl xdr::ToXdr for SECINFO4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(resok)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::ToXdr for secinfo4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::RPCSEC_GSS(gss) => encoder.encode(gss)?,
			Self::Default(u) => encoder.encode(u)?,
		}
		Ok(())
	}
}

impl xdr::ToXdr for rpcsec_gss_info {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.oid)?;
		encoder.encode(&self.qop)?;
		encoder.encode(&self.service)?;
		Ok(())
	}
}

impl xdr::ToXdr for rpc_gss_svc_t {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::RPC_GSS_SVC_NONE => encoder.encode_int(1)?,
			Self::RPC_GSS_SVC_INTEGRITY => encoder.encode_int(2)?,
			Self::RPC_GSS_SVC_PRIVACY => encoder.encode_int(3)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for nfs_client_id4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let verifier = decoder.decode_n()?;
		let id = decoder.decode()?;
		Ok(Self { verifier, id })
	}
}

impl xdr::FromXdr for SETCLIENTID4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let client = decoder.decode()?;
		let callback = decoder.decode()?;
		let callback_ident = decoder.decode()?;
		Ok(Self {
			client,
			callback,
			callback_ident,
		})
	}
}

impl xdr::ToXdr for SETCLIENTID4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::NFS4_OK(resok) => {
				encoder.encode(&nfsstat4::NFS4_OK)?;
				encoder.encode(&resok.clientid)?;
				encoder.encode_n(resok.setclientid_confirm)?;
			},
			Self::NFS4ERR_CLID_INUSE(clientaddr) => {
				encoder.encode(&nfsstat4::NFS4ERR_CLID_INUSE)?;
				encoder.encode(clientaddr)?;
			},
			Self::Error(error) => encoder.encode(error)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for SETCLIENTID_CONFIRM4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let clientid = decoder.decode()?;
		let setclientid_confirm = decoder.decode_n()?;
		Ok(Self {
			clientid,
			setclientid_confirm,
		})
	}
}

impl xdr::ToXdr for SETCLIENTID_CONFIRM4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::FromXdr for RELEASE_LOCKOWNER4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let lock_owner = decoder.decode()?;
		Ok(Self { lock_owner })
	}
}

impl xdr::ToXdr for RELEASE_LOCKOWNER4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for ILLEGAL4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)
	}
}

impl xdr::ToXdr for entry4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.cookie)?;
		encoder.encode(&self.name)?;
		encoder.encode(&self.attrs)?;
		Ok(())
	}
}

impl xdr::FromXdr for nfs_opnum4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let opnum = match decoder.decode_int()? {
			3 => Self::OP_ACCESS,
			4 => Self::OP_CLOSE,
			5 => Self::OP_COMMIT,
			6 => Self::OP_CREATE,
			7 => Self::OP_DELEGPURGE,
			8 => Self::OP_DELEGRETURN,
			9 => Self::OP_GETATTR,
			10 => Self::OP_GETFH,
			11 => Self::OP_LINK,
			12 => Self::OP_LOCK,
			13 => Self::OP_LOCKT,
			14 => Self::OP_LOCKU,
			15 => Self::OP_LOOKUP,
			16 => Self::OP_LOOKUPP,
			17 => Self::OP_NVERIFY,
			18 => Self::OP_OPEN,
			19 => Self::OP_OPENATTR,
			20 => Self::OP_OPEN_CONFIRM,
			21 => Self::OP_OPEN_DOWNGRADE,
			22 => Self::OP_PUTFH,
			23 => Self::OP_PUTPUBFH,
			24 => Self::OP_PUTROOTFH,
			25 => Self::OP_READ,
			26 => Self::OP_READDIR,
			27 => Self::OP_READLINK,
			28 => Self::OP_REMOVE,
			29 => Self::OP_RENAME,
			30 => Self::OP_RENEW,
			31 => Self::OP_RESTOREFH,
			32 => Self::OP_SAVEFH,
			33 => Self::OP_SECINFO,
			34 => Self::OP_SETATTR,
			35 => Self::OP_SETCLIENTID,
			36 => Self::OP_SETCLIENTID_CONFIRM,
			37 => Self::OP_VERIFY,
			38 => Self::OP_WRITE,
			39 => Self::OP_RELEASE_LOCKOWNER,
			opnum => {
				tracing::error!(?opnum, "Illegal opnum.");
				Self::OP_ILLEGAL
			},
		};
		Ok(opnum)
	}
}

impl xdr::ToXdr for nfs_opnum4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			Self::OP_ACCESS => encoder.encode_int(3)?,
			Self::OP_CLOSE => encoder.encode_int(4)?,
			Self::OP_COMMIT => encoder.encode_int(5)?,
			Self::OP_CREATE => encoder.encode_int(6)?,
			Self::OP_DELEGPURGE => encoder.encode_int(7)?,
			Self::OP_DELEGRETURN => encoder.encode_int(8)?,
			Self::OP_GETATTR => encoder.encode_int(9)?,
			Self::OP_GETFH => encoder.encode_int(10)?,
			Self::OP_LINK => encoder.encode_int(11)?,
			Self::OP_LOCK => encoder.encode_int(12)?,
			Self::OP_LOCKT => encoder.encode_int(13)?,
			Self::OP_LOCKU => encoder.encode_int(14)?,
			Self::OP_LOOKUP => encoder.encode_int(15)?,
			Self::OP_LOOKUPP => encoder.encode_int(16)?,
			Self::OP_NVERIFY => encoder.encode_int(17)?,
			Self::OP_OPEN => encoder.encode_int(18)?,
			Self::OP_OPENATTR => encoder.encode_int(19)?,
			Self::OP_OPEN_CONFIRM => encoder.encode_int(20)?,
			Self::OP_OPEN_DOWNGRADE => encoder.encode_int(21)?,
			Self::OP_PUTFH => encoder.encode_int(22)?,
			Self::OP_PUTPUBFH => encoder.encode_int(23)?,
			Self::OP_PUTROOTFH => encoder.encode_int(24)?,
			Self::OP_READ => encoder.encode_int(25)?,
			Self::OP_READDIR => encoder.encode_int(26)?,
			Self::OP_READLINK => encoder.encode_int(27)?,
			Self::OP_REMOVE => encoder.encode_int(28)?,
			Self::OP_RENAME => encoder.encode_int(29)?,
			Self::OP_RENEW => encoder.encode_int(30)?,
			Self::OP_RESTOREFH => encoder.encode_int(31)?,
			Self::OP_SAVEFH => encoder.encode_int(32)?,
			Self::OP_SECINFO => encoder.encode_int(33)?,
			Self::OP_SETATTR => encoder.encode_int(34)?,
			Self::OP_SETCLIENTID => encoder.encode_int(35)?,
			Self::OP_SETCLIENTID_CONFIRM => encoder.encode_int(36)?,
			Self::OP_VERIFY => encoder.encode_int(37)?,
			Self::OP_WRITE => encoder.encode_int(38)?,
			Self::OP_RELEASE_LOCKOWNER => encoder.encode_int(39)?,
			Self::OP_ILLEGAL => encoder.encode_int(10044)?,
		}
		Ok(())
	}
}

impl xdr::FromXdr for nfs_argop4 {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let opnum: nfs_opnum4 = decoder.decode()?;
		let arg = match opnum {
			nfs_opnum4::OP_ACCESS => nfs_argop4::OP_ACCESS(decoder.decode()?),
			nfs_opnum4::OP_CLOSE => nfs_argop4::OP_CLOSE(decoder.decode()?),
			nfs_opnum4::OP_GETATTR => nfs_argop4::OP_GETATTR(decoder.decode()?),
			nfs_opnum4::OP_GETFH => nfs_argop4::OP_GETFH,
			nfs_opnum4::OP_LOCK => nfs_argop4::OP_LOCK(decoder.decode()?),
			nfs_opnum4::OP_LOCKT => nfs_argop4::OP_LOCKT(decoder.decode()?),
			nfs_opnum4::OP_LOCKU => nfs_argop4::OP_LOCKU(decoder.decode()?),
			nfs_opnum4::OP_LOOKUP => nfs_argop4::OP_LOOKUP(decoder.decode()?),
			nfs_opnum4::OP_OPEN => nfs_argop4::OP_OPEN(decoder.decode()?),
			nfs_opnum4::OP_OPENATTR => nfs_argop4::OP_OPENATTR(decoder.decode()?),
			nfs_opnum4::OP_OPEN_CONFIRM => nfs_argop4::OP_OPEN_CONFIRM(decoder.decode()?),
			nfs_opnum4::OP_PUTFH => nfs_argop4::OP_PUTFH(decoder.decode()?),
			nfs_opnum4::OP_PUTROOTFH => nfs_argop4::OP_PUTROOTFH,
			nfs_opnum4::OP_READ => nfs_argop4::OP_READ(decoder.decode()?),
			nfs_opnum4::OP_READDIR => nfs_argop4::OP_READDIR(decoder.decode()?),
			nfs_opnum4::OP_READLINK => nfs_argop4::OP_READLINK,
			nfs_opnum4::OP_RENEW => nfs_argop4::OP_RENEW(decoder.decode()?),
			nfs_opnum4::OP_RESTOREFH => nfs_argop4::OP_RESTOREFH,
			nfs_opnum4::OP_SAVEFH => nfs_argop4::OP_SAVEFH,
			nfs_opnum4::OP_SECINFO => nfs_argop4::OP_SECINFO(decoder.decode()?),
			nfs_opnum4::OP_SETCLIENTID => nfs_argop4::OP_SETCLIENTID(decoder.decode()?),
			nfs_opnum4::OP_SETCLIENTID_CONFIRM => {
				nfs_argop4::OP_SETCLIENTID_CONFIRM(decoder.decode()?)
			},
			nfs_opnum4::OP_RELEASE_LOCKOWNER => nfs_argop4::OP_RELEASE_LOCKOWNER(decoder.decode()?),
			nfs_opnum4::OP_COMMIT => nfs_argop4::OP_COMMIT,
			nfs_opnum4::OP_CREATE => nfs_argop4::OP_CREATE,
			nfs_opnum4::OP_DELEGPURGE => nfs_argop4::OP_DELEGPURGE,
			nfs_opnum4::OP_DELEGRETURN => nfs_argop4::OP_DELEGRETURN,
			nfs_opnum4::OP_REMOVE => nfs_argop4::OP_REMOVE,
			nfs_opnum4::OP_RENAME => nfs_argop4::OP_RENAME,
			nfs_opnum4::OP_LINK => nfs_argop4::OP_LINK,
			nfs_opnum4::OP_LOOKUPP => nfs_argop4::OP_LOOKUPP,
			nfs_opnum4::OP_NVERIFY => nfs_argop4::OP_NVERIFY(decoder.decode()?),
			nfs_opnum4::OP_OPEN_DOWNGRADE => nfs_argop4::OP_OPEN_DOWNGRADE,
			nfs_opnum4::OP_PUTPUBFH => nfs_argop4::OP_PUTPUBFH,
			nfs_opnum4::OP_SETATTR => nfs_argop4::OP_SETATTR,
			nfs_opnum4::OP_VERIFY => nfs_argop4::OP_VERIFY,
			nfs_opnum4::OP_WRITE => nfs_argop4::OP_WRITE,
			nfs_opnum4::OP_ILLEGAL => nfs_argop4::OP_ILLEGAL,
		};
		Ok(arg)
	}
}

impl xdr::FromXdr for COMPOUND4args {
	fn decode(decoder: &mut xdr::Decoder<'_>) -> Result<Self, xdr::Error> {
		let tag = decoder.decode_str()?.to_owned();
		let minorversion = decoder.decode_uint()?;
		let argarray = decoder.decode()?;
		Ok(Self {
			tag,
			minorversion,
			argarray,
		})
	}
}

impl xdr::ToXdr for nfs_resop4 {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		match self {
			nfs_resop4::OP_ACCESS(res) => {
				encoder.encode(&nfs_opnum4::OP_ACCESS)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_CLOSE(res) => {
				encoder.encode(&nfs_opnum4::OP_CLOSE)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_COMMIT => {
				encoder.encode(&nfs_opnum4::OP_COMMIT)?;
				encoder.encode(&nfsstat4::NFS4ERR_NOTSUPP)?;
			},
			nfs_resop4::OP_CREATE => {
				encoder.encode(&nfs_opnum4::OP_CREATE)?;
				encoder.encode(&nfsstat4::NFS4ERR_PERM)?;
			},
			nfs_resop4::OP_DELEGPURGE => {
				encoder.encode(&nfs_opnum4::OP_DELEGPURGE)?;
				encoder.encode(&nfsstat4::NFS4ERR_NOTSUPP)?;
			},
			nfs_resop4::OP_DELEGRETURN => {
				encoder.encode(&nfs_opnum4::OP_DELEGRETURN)?;
				encoder.encode(&nfsstat4::NFS4ERR_NOTSUPP)?;
			},
			nfs_resop4::OP_GETATTR(res) => {
				encoder.encode(&nfs_opnum4::OP_GETATTR)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_GETFH(res) => {
				encoder.encode(&nfs_opnum4::OP_GETFH)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_LINK => {
				encoder.encode(&nfs_opnum4::OP_LINK)?;
				encoder.encode(&nfsstat4::NFS4ERR_PERM)?;
			},
			nfs_resop4::OP_LOCK(res) => {
				encoder.encode(&nfs_opnum4::OP_LOCK)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_LOCKT(res) => {
				encoder.encode(&nfs_opnum4::OP_LOCKT)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_LOCKU(res) => {
				encoder.encode(&nfs_opnum4::OP_LOCKU)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_LOOKUP(res) => {
				encoder.encode(&nfs_opnum4::OP_LOOKUP)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_LOOKUPP(res) => {
				encoder.encode(&nfs_opnum4::OP_LOOKUPP)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_NVERIFY(res) => {
				encoder.encode(&nfs_opnum4::OP_NVERIFY)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_OPEN(res) => {
				encoder.encode(&nfs_opnum4::OP_OPEN)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_OPENATTR(res) => {
				encoder.encode(&nfs_opnum4::OP_OPENATTR)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_OPEN_CONFIRM(res) => {
				encoder.encode(&nfs_opnum4::OP_OPEN_CONFIRM)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_OPEN_DOWNGRADE => {
				encoder.encode(&nfs_opnum4::OP_OPEN_DOWNGRADE)?;
				encoder.encode(&nfsstat4::NFS4ERR_NOTSUPP)?;
			},
			nfs_resop4::OP_PUTFH(res) => {
				encoder.encode(&nfs_opnum4::OP_PUTFH)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_PUTPUBFH(res) => {
				encoder.encode(&nfs_opnum4::OP_PUTPUBFH)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_PUTROOTFH(res) => {
				encoder.encode(&nfs_opnum4::OP_PUTROOTFH)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_READ(res) => {
				encoder.encode(&nfs_opnum4::OP_READ)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_READDIR(res) => {
				encoder.encode(&nfs_opnum4::OP_READDIR)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_READLINK(res) => {
				encoder.encode(&nfs_opnum4::OP_READLINK)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_REMOVE => {
				encoder.encode(&nfs_opnum4::OP_REMOVE)?;
				encoder.encode(&nfsstat4::NFS4ERR_PERM)?;
			},
			nfs_resop4::OP_RENAME => {
				encoder.encode(&nfs_opnum4::OP_RENAME)?;
				encoder.encode(&nfsstat4::NFS4ERR_PERM)?;
			},
			nfs_resop4::OP_RENEW(res) => {
				encoder.encode(&nfs_opnum4::OP_RENEW)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_RESTOREFH(res) => {
				encoder.encode(&nfs_opnum4::OP_RESTOREFH)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_SAVEFH(res) => {
				encoder.encode(&nfs_opnum4::OP_SAVEFH)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_SECINFO(res) => {
				encoder.encode(&nfs_opnum4::OP_SECINFO)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_SETATTR => {
				encoder.encode(&nfs_opnum4::OP_SETATTR)?;
				encoder.encode(&nfsstat4::NFS4ERR_PERM)?;
			},
			nfs_resop4::OP_SETCLIENTID(res) => {
				encoder.encode(&nfs_opnum4::OP_SETCLIENTID)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_SETCLIENTID_CONFIRM(res) => {
				encoder.encode(&nfs_opnum4::OP_SETCLIENTID_CONFIRM)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_VERIFY => {
				encoder.encode(&nfs_opnum4::OP_VERIFY)?;
				encoder.encode(&nfsstat4::NFS4ERR_NOTSUPP)?;
			},
			nfs_resop4::OP_WRITE => {
				encoder.encode(&nfs_opnum4::OP_WRITE)?;
				encoder.encode(&nfsstat4::NFS4ERR_PERM)?;
			},
			nfs_resop4::OP_RELEASE_LOCKOWNER(res) => {
				encoder.encode(&nfs_opnum4::OP_RELEASE_LOCKOWNER)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::OP_ILLEGAL(res) => {
				encoder.encode(&nfs_opnum4::OP_ILLEGAL)?;
				encoder.encode(&res)?;
			},
			nfs_resop4::Unknown(opnum) => {
				encoder.encode(opnum)?;
				encoder.encode(&nfsstat4::NFS4ERR_NOTSUPP)?;
			},
			nfs_resop4::Timeout(opnum) => {
				encoder.encode(opnum)?;
				encoder.encode(&nfsstat4::NFS4ERR_DELAY)?;
			},
		}
		Ok(())
	}
}

impl xdr::ToXdr for COMPOUND4res {
	fn encode<W>(&self, encoder: &mut xdr::Encoder<W>) -> Result<(), xdr::Error>
	where
		W: std::io::Write,
	{
		encoder.encode(&self.status)?;
		encoder.encode_str(&self.tag)?;
		encoder.encode(&self.resarray)?;
		Ok(())
	}
}

impl bitmap4 {
	pub fn set(&mut self, bit: usize) {
		let word = bit / 32;
		if word >= self.0.len() {
			self.0.resize_with(word + 1, || 0);
		}
		self.0[word] |= 1 << (bit % 32);
	}

	pub fn get(&self, bit: usize) -> bool {
		let word = self.0.get(bit / 32).copied().unwrap_or(0);
		let flag = 1 & (word >> (bit % 32));
		flag != 0
	}

	pub fn intersection(&self, rhs: &Self) -> Self {
		let sz = self.0.len().max(rhs.0.len());
		let mut new = vec![0; sz];
		for (i, new) in new.iter_mut().enumerate() {
			let lhs = self.0.get(i).copied().unwrap_or(0);
			let rhs = rhs.0.get(i).copied().unwrap_or(0);
			*new = lhs & rhs;
		}
		Self(new)
	}
}

impl nfstime4 {
	pub fn new() -> nfstime4 {
		nfstime4 {
			seconds: 0,
			nseconds: 0,
		}
	}

	pub fn now() -> nfstime4 {
		let now = std::time::SystemTime::now();
		let dur = now.duration_since(std::time::UNIX_EPOCH).unwrap();
		Self {
			seconds: dur.as_secs().to_i64().unwrap(),
			nseconds: dur.subsec_nanos(),
		}
	}
}

impl From<std::io::Error> for nfsstat4 {
	// https://github.com/apple/darwin-xnu/blob/main/bsd/sys/errno.h
	// https://www.thegeekstuff.com/2010/10/linux-error-codes/
	fn from(value: std::io::Error) -> Self {
		match value.raw_os_error() {
			Some(0) => nfsstat4::NFS4_OK,
			Some(1) => nfsstat4::NFS4ERR_PERM,
			Some(2) => nfsstat4::NFS4ERR_NOENT,
			Some(5) => nfsstat4::NFS4ERR_IO,
			Some(6) => nfsstat4::NFS4ERR_NXIO,
			Some(13) => nfsstat4::NFS4ERR_ACCESS,
			Some(17) => nfsstat4::NFS4ERR_EXIST,
			Some(18) => nfsstat4::NFS4ERR_XDEV,
			Some(20) => nfsstat4::NFS4ERR_NOTDIR,
			Some(21) => nfsstat4::NFS4ERR_ISDIR,
			Some(22) => nfsstat4::NFS4ERR_INVAL,
			Some(27) => nfsstat4::NFS4ERR_FBIG,
			Some(28) => nfsstat4::NFS4ERR_NOSPC,
			Some(30) => nfsstat4::NFS4ERR_ROFS,
			Some(31) => nfsstat4::NFS4ERR_MLINK,
			Some(63) => nfsstat4::NFS4ERR_NAMETOOLONG,
			Some(66) => nfsstat4::NFS4ERR_NOTEMPTY,
			Some(69) => nfsstat4::NFS4ERR_DQUOT,
			Some(70) => nfsstat4::NFS4ERR_STALE,
			_ => nfsstat4::NFS4ERR_IO,
		}
	}
}

impl nfs_resop4 {
	pub fn status(&self) -> nfsstat4 {
		match self {
			nfs_resop4::OP_ACCESS(ACCESS4res::Error(e)) => *e,
			nfs_resop4::OP_ACCESS(ACCESS4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_CLOSE(CLOSE4res::Error(e)) => *e,
			nfs_resop4::OP_CLOSE(CLOSE4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_COMMIT => nfsstat4::NFS4ERR_NOTSUPP,
			nfs_resop4::OP_CREATE => nfsstat4::NFS4ERR_PERM,
			nfs_resop4::OP_DELEGPURGE => nfsstat4::NFS4ERR_NOTSUPP,
			nfs_resop4::OP_DELEGRETURN => nfsstat4::NFS4ERR_NOTSUPP,
			nfs_resop4::OP_GETATTR(GETATTR4res::Error(e)) => *e,
			nfs_resop4::OP_GETATTR(GETATTR4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_GETFH(GETFH4res::Error(e)) => *e,
			nfs_resop4::OP_GETFH(GETFH4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_ILLEGAL(_) => nfsstat4::NFS4ERR_OP_ILLEGAL,
			nfs_resop4::OP_LINK => nfsstat4::NFS4ERR_PERM,
			nfs_resop4::OP_LOCK(LOCK4res::Error(e)) => *e,
			nfs_resop4::OP_LOCK(LOCK4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_LOCK(LOCK4res::NFS4ERR_DENIED(_)) => nfsstat4::NFS4ERR_DENIED,
			nfs_resop4::OP_LOCKT(LOCKT4res::Error(e)) => *e,
			nfs_resop4::OP_LOCKT(LOCKT4res::NFS4_OK) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_LOCKT(LOCKT4res::NFS4ERR_DENIED(_)) => nfsstat4::NFS4ERR_DENIED,
			nfs_resop4::OP_LOCKU(LOCKU4res::Error(e)) => *e,
			nfs_resop4::OP_LOCKU(LOCKU4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_LOOKUP(LOOKUP4res { status }) => *status,
			nfs_resop4::OP_LOOKUPP(res) => res.status,
			nfs_resop4::OP_NVERIFY(res) => res.status,
			nfs_resop4::OP_OPEN_CONFIRM(OPEN_CONFIRM4res::Error(e)) => *e,
			nfs_resop4::OP_OPEN_CONFIRM(OPEN_CONFIRM4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_OPEN_DOWNGRADE => nfsstat4::NFS4ERR_NOTSUPP,
			nfs_resop4::OP_OPEN(OPEN4res::Error(e)) => *e,
			nfs_resop4::OP_OPEN(OPEN4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_OPENATTR(res) => res.status,
			nfs_resop4::OP_PUTFH(PUTFH4res { status }) => *status,
			nfs_resop4::OP_PUTPUBFH(res) => res.status,
			nfs_resop4::OP_PUTROOTFH(PUTROOTFH4res { status }) => *status,
			nfs_resop4::OP_READ(READ4res::Error(e)) => *e,
			nfs_resop4::OP_READ(READ4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_READDIR(READDIR4res::Error(e)) => *e,
			nfs_resop4::OP_READDIR(READDIR4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_READLINK(READLINK4res::Error(e)) => *e,
			nfs_resop4::OP_READLINK(READLINK4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_REMOVE => nfsstat4::NFS4ERR_PERM,
			nfs_resop4::OP_RENAME => nfsstat4::NFS4ERR_PERM,
			nfs_resop4::OP_RENEW(RENEW4res { status }) => *status,
			nfs_resop4::OP_RESTOREFH(RESTOREFH4res { status }) => *status,
			nfs_resop4::OP_SAVEFH(SAVEFH4res { status }) => *status,
			nfs_resop4::OP_SECINFO(SECINFO4res::Error(e)) => *e,
			nfs_resop4::OP_SECINFO(SECINFO4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_SETATTR => nfsstat4::NFS4ERR_PERM,
			nfs_resop4::OP_SETCLIENTID_CONFIRM(SETCLIENTID_CONFIRM4res { status }) => *status,
			nfs_resop4::OP_SETCLIENTID(SETCLIENTID4res::Error(e)) => *e,
			nfs_resop4::OP_SETCLIENTID(SETCLIENTID4res::NFS4_OK(_)) => nfsstat4::NFS4_OK,
			nfs_resop4::OP_SETCLIENTID(SETCLIENTID4res::NFS4ERR_CLID_INUSE(_)) => {
				nfsstat4::NFS4ERR_CLID_INUSE
			},
			nfs_resop4::OP_VERIFY => nfsstat4::NFS4ERR_NOTSUPP,
			nfs_resop4::OP_WRITE => nfsstat4::NFS4ERR_PERM,
			nfs_resop4::OP_RELEASE_LOCKOWNER(RELEASE_LOCKOWNER4res { status }) => *status,
			nfs_resop4::Unknown(_) => nfsstat4::NFS4ERR_NOTSUPP,
			nfs_resop4::Timeout(_) => nfsstat4::NFS4ERR_DELAY,
		}
	}
}

impl std::fmt::Debug for bitmap4 {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let mut list = f.debug_list();
		for flag in ALL_ATTRS {
			if !self.get(flag.to_usize().unwrap()) {
				continue;
			}
			match flag {
				FATTR4_SUPPORTED_ATTRS => list.entry(&"SUPPORTED_ATTRS"),
				FATTR4_TYPE => list.entry(&"TYPE"),
				FATTR4_FH_EXPIRE_TYPE => list.entry(&"FH_EXPIRE_TYPE"),
				FATTR4_CHANGE => list.entry(&"CHANGE"),
				FATTR4_SIZE => list.entry(&"SIZE"),
				FATTR4_LINK_SUPPORT => list.entry(&"LINK_SUPPORT"),
				FATTR4_SYMLINK_SUPPORT => list.entry(&"SYMLINK_SUPPORT"),
				FATTR4_NAMED_ATTR => list.entry(&"NAMED_ATTR"),
				FATTR4_FSID => list.entry(&"FSID"),
				FATTR4_UNIQUE_HANDLES => list.entry(&"UNIQUE_HANDLES"),
				FATTR4_LEASE_TIME => list.entry(&"LEASE_TIME"),
				FATTR4_RDATTR_ERROR => list.entry(&"RDATTR_ERROR"),
				FATTR4_ACL => list.entry(&"ACL"),
				FATTR4_ACLSUPPORT => list.entry(&"ACLSUPPORT"),
				FATTR4_ARCHIVE => list.entry(&"ARCHIVE"),
				FATTR4_CANSETTIME => list.entry(&"CANSETTIME"),
				FATTR4_CASE_INSENSITIVE => list.entry(&"CASE_INSENSITIVE"),
				FATTR4_CASE_PRESERVING => list.entry(&"CASE_PRESERVING"),
				FATTR4_CHOWN_RESTRICTED => list.entry(&"CHOWN_RESTRICTED"),
				FATTR4_FILEHANDLE => list.entry(&"FILEHANDLE"),
				FATTR4_FILEID => list.entry(&"FILEID"),
				FATTR4_FILES_AVAIL => list.entry(&"FILES_AVAIL"),
				FATTR4_FILES_FREE => list.entry(&"FILES_FREE"),
				FATTR4_FILES_TOTAL => list.entry(&"FILES_TOTAL"),
				FATTR4_FS_LOCATIONS => list.entry(&"FS_LOCATIONS"),
				FATTR4_HIDDEN => list.entry(&"HIDDEN"),
				FATTR4_HOMOGENEOUS => list.entry(&"HOMOGENEOUS"),
				FATTR4_MAXFILESIZE => list.entry(&"MAXFILESIZE"),
				FATTR4_MAXLINK => list.entry(&"MAXLINK"),
				FATTR4_MAXNAME => list.entry(&"MAXNAME"),
				FATTR4_MAXREAD => list.entry(&"MAXREAD"),
				FATTR4_MAXWRITE => list.entry(&"MAXWRITE"),
				FATTR4_MIMETYPE => list.entry(&"MIMETYPE"),
				FATTR4_MODE => list.entry(&"MODE"),
				FATTR4_NO_TRUNC => list.entry(&"NO_TRUNC"),
				FATTR4_NUMLINKS => list.entry(&"NUMLINKS"),
				FATTR4_OWNER => list.entry(&"OWNER"),
				FATTR4_OWNER_GROUP => list.entry(&"OWNER_GROUP"),
				FATTR4_QUOTA_AVAIL_HARD => list.entry(&"QUOTA_AVAIL_HARD"),
				FATTR4_QUOTA_AVAIL_SOFT => list.entry(&"QUOTA_AVAIL_SOFT"),
				FATTR4_QUOTA_USED => list.entry(&"QUOTA_USED"),
				FATTR4_RAWDEV => list.entry(&"RAWDEV"),
				FATTR4_SPACE_AVAIL => list.entry(&"SPACE_AVAIL"),
				FATTR4_SPACE_FREE => list.entry(&"SPACE_FREE"),
				FATTR4_SPACE_TOTAL => list.entry(&"SPACE_TOTAL"),
				FATTR4_SPACE_USED => list.entry(&"SPACE_USED"),
				FATTR4_SYSTEM => list.entry(&"SYSTEM"),
				FATTR4_TIME_ACCESS => list.entry(&"TIME_ACCESS"),
				// FATTR4_TIME_ACCESS_SET => list.entry(&"TIME_ACCESS_SET"),
				FATTR4_TIME_BACKUP => list.entry(&"TIME_BACKUP"),
				FATTR4_TIME_CREATE => list.entry(&"TIME_CREATE"),
				FATTR4_TIME_DELTA => list.entry(&"TIME_DELTA"),
				FATTR4_TIME_METADATA => list.entry(&"TIME_METADATA"),
				FATTR4_TIME_MODIFY => list.entry(&"TIME_MODIFY"),
				FATTR4_TIME_MODIFY_SET => list.entry(&"TIME_MODIFY_SET"),
				FATTR4_MOUNTED_ON_FILEID => list.entry(&"MOUNTED_ON_FILEID"),
				FATTR4_DIR_NOTIF_DELAY => list.entry(&"DIR_NOTIF_DELAY"),
				FATTR4_DIRENT_NOTIF_DELAY => list.entry(&"DIRENT_NOTIF_DELAY"),
				FATTR4_DACL => list.entry(&"DACL"),
				FATTR4_SACL => list.entry(&"SACL"),
				FATTR4_CHANGE_POLICY => list.entry(&"CHANGE_POLICY"),
				FATTR4_FS_STATUS => list.entry(&"FS_STATUS"),
				FATTR4_FS_LAYOUT_TYPE => list.entry(&"FS_LAYOUT_TYPE"),
				FATTR4_LAYOUT_HINT => list.entry(&"LAYOUT_HINT"),
				FATTR4_LAYOUT_TYPE => list.entry(&"LAYOUT_TYPE"),
				FATTR4_LAYOUT_BLKSIZE => list.entry(&"LAYOUT_BLKSIZE"),
				FATTR4_LAYOUT_ALIGNMENT => list.entry(&"LAYOUT_ALIGNMENT"),
				FATTR4_FS_LOCATIONS_INFO => list.entry(&"FS_LOCATIONS_INFO"),
				FATTR4_MDSTHRESHOLD => list.entry(&"MDSTHRESHOLD"),
				FATTR4_RETENTION_GET => list.entry(&"RETENTION_GET"),
				FATTR4_RETENTION_SET => list.entry(&"RETENTION_SET"),
				FATTR4_RETENTEVT_GET => list.entry(&"RETENTEVT_GET"),
				FATTR4_RETENTEVT_SET => list.entry(&"RETENTEVT_SET"),
				FATTR4_RETENTION_HOLD => list.entry(&"RETENTION_HOLD"),
				FATTR4_MODE_SET_MASKED => list.entry(&"MODE_SET_MASKED"),
				FATTR4_SUPPATTR_EXCLCREAT => list.entry(&"SUPPATTR_EXCLCREAT"),
				FATTR4_FS_CHARSET_CAP => list.entry(&"FS_CHARSET_CA&P"),
				_ => continue,
			};
		}
		list.finish()
	}
}

// List of all attributes.
pub const ALL_ATTRS: [u32; 77] = [
	FATTR4_SUPPORTED_ATTRS,
	FATTR4_TYPE,
	FATTR4_FH_EXPIRE_TYPE,
	FATTR4_CHANGE,
	FATTR4_SIZE,
	FATTR4_LINK_SUPPORT,
	FATTR4_SYMLINK_SUPPORT,
	FATTR4_NAMED_ATTR,
	FATTR4_FSID,
	FATTR4_UNIQUE_HANDLES,
	FATTR4_LEASE_TIME,
	FATTR4_RDATTR_ERROR,
	FATTR4_ACL,
	FATTR4_ACLSUPPORT,
	FATTR4_ARCHIVE,
	FATTR4_CANSETTIME,
	FATTR4_CASE_INSENSITIVE,
	FATTR4_CASE_PRESERVING,
	FATTR4_CHOWN_RESTRICTED,
	FATTR4_FILEHANDLE,
	FATTR4_FILEID,
	FATTR4_FILES_AVAIL,
	FATTR4_FILES_FREE,
	FATTR4_FILES_TOTAL,
	FATTR4_FS_LOCATIONS,
	FATTR4_HIDDEN,
	FATTR4_HOMOGENEOUS,
	FATTR4_MAXFILESIZE,
	FATTR4_MAXLINK,
	FATTR4_MAXNAME,
	FATTR4_MAXREAD,
	FATTR4_MAXWRITE,
	FATTR4_MIMETYPE,
	FATTR4_MODE,
	FATTR4_NO_TRUNC,
	FATTR4_NUMLINKS,
	FATTR4_OWNER,
	FATTR4_OWNER_GROUP,
	FATTR4_QUOTA_AVAIL_HARD,
	FATTR4_QUOTA_AVAIL_SOFT,
	FATTR4_QUOTA_USED,
	FATTR4_RAWDEV,
	FATTR4_SPACE_AVAIL,
	FATTR4_SPACE_FREE,
	FATTR4_SPACE_TOTAL,
	FATTR4_SPACE_USED,
	FATTR4_SYSTEM,
	FATTR4_TIME_ACCESS,
	FATTR4_TIME_ACCESS_SET,
	FATTR4_TIME_BACKUP,
	FATTR4_TIME_CREATE,
	FATTR4_TIME_DELTA,
	FATTR4_TIME_METADATA,
	FATTR4_TIME_MODIFY,
	FATTR4_TIME_MODIFY_SET,
	FATTR4_MOUNTED_ON_FILEID,
	FATTR4_DIR_NOTIF_DELAY,
	FATTR4_DIRENT_NOTIF_DELAY,
	FATTR4_DACL,
	FATTR4_SACL,
	FATTR4_CHANGE_POLICY,
	FATTR4_FS_STATUS,
	FATTR4_FS_LAYOUT_TYPE,
	FATTR4_LAYOUT_HINT,
	FATTR4_LAYOUT_TYPE,
	FATTR4_LAYOUT_BLKSIZE,
	FATTR4_LAYOUT_ALIGNMENT,
	FATTR4_FS_LOCATIONS_INFO,
	FATTR4_MDSTHRESHOLD,
	FATTR4_RETENTION_GET,
	FATTR4_RETENTION_SET,
	FATTR4_RETENTEVT_GET,
	FATTR4_RETENTEVT_SET,
	FATTR4_RETENTION_HOLD,
	FATTR4_MODE_SET_MASKED,
	FATTR4_SUPPATTR_EXCLCREAT,
	FATTR4_FS_CHARSET_CAP,
];

#[cfg(test)]
mod test {
	use super::{xdr::ToXdr, *};

	#[test]
	fn setclientid() {
		let res = SETCLIENTID4res::NFS4_OK(super::SETCLIENTID4resok {
			clientid: 1007,
			setclientid_confirm: [0, 0, 0, 0, 0, 0, 3, 239],
		});

		let compound = COMPOUND4res {
			status: nfsstat4::NFS4_OK,
			tag: String::from_utf8(vec![115, 101, 116, 99, 108, 105, 100, 32, 32, 32, 32, 32])
				.unwrap(),
			resarray: vec![nfs_resop4::OP_SETCLIENTID(res)],
		};

		let mut buffer = vec![];
		let mut encoder = xdr::Encoder::new(&mut buffer);
		compound
			.encode(&mut encoder)
			.expect("Failed to encode result.");
	}
}
