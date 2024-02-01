use crate::nfs::types::nfs_opnum4;

use self::types::{
	bitmap4, cb_client4, change_info4, dirlist4, entry4, fattr4, fs_locations4, fsid4, length4,
	locker4, nfs_argop4, nfs_client_id4, nfs_fh4, nfs_ftype4, nfs_lock_type4, nfs_resop4, nfsace4,
	nfsstat4, nfstime4, offset4, open_claim4, open_delegation4, open_delegation_type4, pathname4,
	specdata4, stateid4, verifier4, ACCESS4args, ACCESS4res, ACCESS4resok, CLOSE4args, CLOSE4res,
	COMPOUND4res, GETATTR4args, GETATTR4res, GETATTR4resok, GETFH4res, GETFH4resok, LOCK4args,
	LOCK4res, LOCK4resok, LOCKT4args, LOCKT4res, LOCKU4args, LOCKU4res, LOOKUP4args, LOOKUP4res,
	LOOKUPP4res, NVERIFY4res, OPEN4args, OPEN4res, OPEN4resok, OPENATTR4args, OPENATTR4res,
	OPEN_CONFIRM4args, OPEN_CONFIRM4res, OPEN_CONFIRM4resok, PUTFH4args, PUTFH4res, READ4args,
	READ4res, READ4resok, READDIR4args, READDIR4res, READDIR4resok, READLINK4res, READLINK4resok,
	RELEASE_LOCKOWNER4args, RELEASE_LOCKOWNER4res, RENEW4args, RENEW4res, RESTOREFH4res,
	SAVEFH4res, SECINFO4args, SECINFO4res, SETCLIENTID4args, SETCLIENTID4res, SETCLIENTID4resok,
	SETCLIENTID_CONFIRM4args, SETCLIENTID_CONFIRM4res, ACCESS4_EXECUTE, ACCESS4_LOOKUP,
	ACCESS4_READ, ANONYMOUS_STATE_ID, FATTR4_ACL, FATTR4_ACLSUPPORT, FATTR4_ARCHIVE,
	FATTR4_CANSETTIME, FATTR4_CASE_INSENSITIVE, FATTR4_CASE_PRESERVING, FATTR4_CHANGE,
	FATTR4_CHOWN_RESTRICTED, FATTR4_FH_EXPIRE_TYPE, FATTR4_FILEHANDLE, FATTR4_FILEID,
	FATTR4_FILES_AVAIL, FATTR4_FILES_FREE, FATTR4_FILES_TOTAL, FATTR4_FSID, FATTR4_FS_LOCATIONS,
	FATTR4_HIDDEN, FATTR4_HOMOGENEOUS, FATTR4_LEASE_TIME, FATTR4_LINK_SUPPORT, FATTR4_MAXFILESIZE,
	FATTR4_MAXLINK, FATTR4_MAXNAME, FATTR4_MAXREAD, FATTR4_MAXWRITE, FATTR4_MIMETYPE, FATTR4_MODE,
	FATTR4_MOUNTED_ON_FILEID, FATTR4_NAMED_ATTR, FATTR4_NO_TRUNC, FATTR4_NUMLINKS, FATTR4_OWNER,
	FATTR4_OWNER_GROUP, FATTR4_QUOTA_AVAIL_HARD, FATTR4_QUOTA_AVAIL_SOFT, FATTR4_QUOTA_USED,
	FATTR4_RAWDEV, FATTR4_RDATTR_ERROR, FATTR4_SIZE, FATTR4_SPACE_AVAIL, FATTR4_SPACE_FREE,
	FATTR4_SPACE_TOTAL, FATTR4_SPACE_USED, FATTR4_SUPPORTED_ATTRS, FATTR4_SYMLINK_SUPPORT,
	FATTR4_SYSTEM, FATTR4_TIME_ACCESS, FATTR4_TIME_BACKUP, FATTR4_TIME_CREATE, FATTR4_TIME_DELTA,
	FATTR4_TIME_METADATA, FATTR4_TIME_MODIFY, FATTR4_TYPE, FATTR4_UNIQUE_HANDLES, MODE4_RGRP,
	MODE4_ROTH, MODE4_RUSR, MODE4_XGRP, MODE4_XOTH, MODE4_XUSR, NFS4_VERIFIER_SIZE, NFS_PROG,
	NFS_VERS, OPEN4_RESULT_CONFIRM, OPEN4_RESULT_LOCKTYPE_POSIX, OPEN4_SHARE_ACCESS_BOTH,
	OPEN4_SHARE_ACCESS_WRITE, READ_BYPASS_STATE_ID, RPC_VERS,
};
use either::Either;
use fnv::FnvBuildHasher;
use num::ToPrimitive;
use std::{
	collections::HashMap,
	fmt,
	os::unix::ffi::OsStrExt,
	path::{Path, PathBuf},
	sync::{Arc, Weak},
};
use tangram_client as tg;
use tangram_error::{Result, WrapErr};
use tokio::{
	io::{AsyncReadExt, AsyncSeekExt},
	net::{TcpListener, TcpStream},
};

mod rpc;
mod types;
mod xdr;

const ROOT: nfs_fh4 = nfs_fh4(0);
type Map<K, V> = HashMap<K, V, FnvBuildHasher>;

#[derive(Clone)]
pub struct Server {
	inner: Arc<Inner>,
}

struct Inner {
	tg: Box<dyn tg::Handle>,
	path: PathBuf,
	task: std::sync::Mutex<Option<tokio::task::JoinHandle<Result<()>>>>,
	clients: tokio::sync::RwLock<Map<Vec<u8>, Arc<tokio::sync::RwLock<ClientData>>>>,
	client_index: std::sync::atomic::AtomicU64,
	nodes: tokio::sync::RwLock<Map<u64, Arc<Node>>>,
	node_index: std::sync::atomic::AtomicU64,
	lock_index: tokio::sync::RwLock<u64>,
	locks: tokio::sync::RwLock<Map<u64, Arc<tokio::sync::RwLock<LockState>>>>,
}

struct LockState {
	reader: Option<tg::blob::Reader>,
	fh: nfs_fh4,
	byterange_locks: Vec<(offset4, length4)>,
}

#[derive(Debug)]
struct Node {
	id: u64,
	parent: Weak<Self>,
	kind: NodeKind,
}

/// An node's kind.
#[derive(Debug)]
enum NodeKind {
	Root {
		children: tokio::sync::RwLock<Map<String, Arc<Node>>>,
		attributes: tokio::sync::RwLock<Option<Arc<Node>>>,
	},
	Directory {
		directory: tg::Directory,
		children: tokio::sync::RwLock<Map<String, Arc<Node>>>,
		attributes: tokio::sync::RwLock<Option<Arc<Node>>>,
	},
	File {
		file: tg::File,
		size: u64,
		attributes: tokio::sync::RwLock<Option<Arc<Node>>>,
	},
	Symlink {
		symlink: tg::Symlink,
		attributes: tokio::sync::RwLock<Option<Arc<Node>>>,
	},
	NamedAttribute {
		data: Vec<u8>,
	},
	NamedAttributeDirectory {
		children: tokio::sync::RwLock<Map<String, Arc<Node>>>,
	},
	Checkout {
		path: PathBuf,
	},
}

pub struct ClientData {
	pub server_id: u64,
	pub client_verifier: verifier4,
	pub server_verifier: verifier4,
	pub callback: cb_client4,
	pub callback_ident: u32,
	pub confirmed: bool,
}

#[derive(Debug, Clone)]
pub struct Context {
	#[allow(dead_code)]
	minor_version: u32,
	current_file_handle: Option<nfs_fh4>,
	saved_file_handle: Option<nfs_fh4>,
}

impl fmt::Display for Context {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "(current: ")?;
		if let Some(current) = self.current_file_handle {
			write!(f, "{}", current.0)?;
		} else {
			write!(f, "nil")?;
		}
		write!(f, " saved: ")?;
		if let Some(saved) = self.saved_file_handle {
			write!(f, "{}", saved.0)?;
		} else {
			write!(f, "nil")?;
		}
		write!(f, ")")
	}
}

impl Server {
	pub async fn start(tg: &dyn tg::Handle, path: &Path, port: u16) -> Result<Self> {
		// Create the server.
		let tg = tg.clone_box();
		let root = Arc::new_cyclic(|root| Node {
			id: 0,
			parent: root.clone(),
			kind: NodeKind::Root {
				children: tokio::sync::RwLock::new(Map::default()),
				attributes: tokio::sync::RwLock::new(None),
			},
		});
		let nodes = [(0, root)].into_iter().collect();
		let task = std::sync::Mutex::new(None);
		let server = Self {
			inner: Arc::new(Inner {
				tg,
				path: path.to_owned(),
				nodes: tokio::sync::RwLock::new(nodes),
				node_index: std::sync::atomic::AtomicU64::new(1000),
				lock_index: tokio::sync::RwLock::new(1),
				client_index: std::sync::atomic::AtomicU64::new(1),
				clients: tokio::sync::RwLock::new(Map::default()),
				locks: tokio::sync::RwLock::new(Map::default()),
				task,
			}),
		};

		// Spawn the task.
		let task = tokio::spawn({
			let server = server.clone();
			async move {
				if let Err(error) = server.serve(port).await {
					tracing::error!(?error, "NFS server shutdown.");
					Err(error)
				} else {
					Ok(())
				}
			}
		});
		server.inner.task.lock().unwrap().replace(task);

		// Mount.
		Self::mount(path, port).await?;

		Ok(server)
	}

	async fn mount(path: &Path, port: u16) -> crate::Result<()> {
		Self::unmount(path).await?;

		tokio::process::Command::new("dns-sd")
			.args([
				"-P",
				"Tangram",
				"_nfs._tcp",
				"local",
				&port.to_string(),
				"Tangram",
				"::1",
				"path=/",
			])
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.spawn()
			.wrap_err("Failed to spawn dns-sd.")?;

		tokio::process::Command::new("mount_nfs")
			.arg("-o")
			.arg(format!(
				"async,actimeo=60,mutejukebox,noquota,async,noacl,rdonly,rsize=2097152,nocallback,tcp,vers=4.0,namedattr,port={port}"
			))
			.arg("Tangram:/")
			.arg(path)
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await
			.wrap_err("Failed to mount.")?
			.success()
			.then_some(())
			.wrap_err("Failed to mount the VFS.")?;

		Ok(())
	}

	async fn unmount(path: &Path) -> Result<()> {
		tokio::process::Command::new("umount")
			.arg("-f")
			.arg(path)
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await
			.wrap_err("Failed to unmount the VFS.")?;
		Ok(())
	}

	pub fn stop(&self) {
		// Abort the task.
		if let Some(task) = self.inner.task.lock().unwrap().as_ref() {
			task.abort_handle().abort();
		};
	}

	pub async fn join(&self) -> Result<()> {
		// Join the task.
		let task = self.inner.task.lock().unwrap().take();
		if let Some(task) = task {
			match task.await {
				Ok(result) => Ok(result),
				Err(error) if error.is_cancelled() => Ok(Ok(())),
				Err(error) => Err(error),
			}
			.unwrap()?;
		}

		// Unmount.
		Self::unmount(&self.inner.path).await?;

		Ok(())
	}

	async fn serve(&self, port: u16) -> crate::Result<()> {
		let listener = TcpListener::bind(format!("localhost:{port}"))
			.await
			.wrap_err("Failed to bind the server.")?;

		loop {
			let (conn, addr) = listener
				.accept()
				.await
				.wrap_err("Failed to accept the connection.")?;
			tracing::info!(?addr, "Accepted client connection.");
			let server = self.clone();
			tokio::spawn(async move {
				if let Err(error) = server.handle_connection(conn).await {
					tracing::error!(?addr, ?error, "The connection was closed.");
				}
			});
		}
	}

	async fn handle_connection(&self, stream: TcpStream) -> Result<()> {
		let (mut reader, mut writer) = tokio::io::split(stream);

		let (message_sender, mut message_receiver) =
			tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();

		// Create the writer task.
		tokio::spawn(async move {
			while let Some(message) = message_receiver.recv().await {
				rpc::write_fragments(&mut writer, &message).await?;
			}
			Ok::<_, xdr::Error>(())
		});

		// Receive incoming message fragments.
		loop {
			let fragments = rpc::read_fragments(&mut reader)
				.await
				.wrap_err("Failed to read message fragments.")?;
			let message_sender = message_sender.clone();
			let vfs = self.clone();
			tokio::task::spawn(async move {
				let mut decoder = xdr::Decoder::from_bytes(&fragments);
				let mut buffer = Vec::with_capacity(4096);
				let mut encoder = xdr::Encoder::new(&mut buffer);
				while let Ok(message) = decoder.decode::<rpc::Message>() {
					let xid = message.xid;
					let Some(body) = vfs.handle_message(message, &mut decoder).await else {
						continue;
					};
					let body = rpc::MessageBody::Reply(body);
					let message = rpc::Message { xid, body };
					encoder.encode(&message).unwrap();
				}
				message_sender.send(buffer).unwrap();
			});
		}
	}

	async fn handle_message(
		&self,
		message: rpc::Message,
		decoder: &mut xdr::Decoder<'_>,
	) -> Option<rpc::ReplyBody> {
		match message.clone().body {
			rpc::MessageBody::Call(call) => {
				if call.rpcvers != RPC_VERS {
					tracing::error!(?call, "Version mismatch.");
					let rejected = rpc::ReplyRejected::RpcMismatch {
						low: RPC_VERS,
						high: RPC_VERS,
					};
					let body = rpc::ReplyBody::Rejected(rejected);
					return Some(body);
				}

				if call.vers != NFS_VERS {
					tracing::error!(?call, "Program mismatch.");
					return Some(rpc::error(
						None,
						rpc::ReplyAcceptedStat::ProgramMismatch {
							low: NFS_VERS,
							high: NFS_VERS,
						},
					));
				}

				if call.prog != NFS_PROG {
					tracing::error!(?call, "Expected NFS4_PROGRAM but got {}.", call.prog);
					return Some(rpc::error(None, rpc::ReplyAcceptedStat::ProgramUnavailable));
				}

				let reply = match call.proc {
					0 => self.handle_null(),
					1 => {
						self.handle_compound(message.xid, call.cred, call.verf, decoder)
							.await
					},
					_ => rpc::error(None, rpc::ReplyAcceptedStat::ProcedureUnavailable),
				};

				Some(reply)
			},
			rpc::MessageBody::Reply(reply) => {
				tracing::warn!(?reply, "Ignoring reply");
				None
			},
		}
	}

	// Check if credential and verification are valid.
	#[allow(clippy::unused_async, clippy::unnecessary_wraps)]
	async fn handle_auth(
		&self,
		_cred: rpc::Auth,
		_verf: rpc::Auth,
	) -> Result<Option<rpc::Auth>, rpc::AuthStat> {
		Ok(None)
	}

	fn handle_null(&self) -> rpc::ReplyBody {
		rpc::success(None, ())
	}

	// See <https://datatracker.ietf.org/doc/html/rfc7530#section-17.2>.
	#[allow(clippy::too_many_lines)]
	async fn handle_compound(
		&self,
		xid: u32,
		cred: rpc::Auth,
		verf: rpc::Auth,
		decoder: &mut xdr::Decoder<'_>,
	) -> rpc::ReplyBody {
		// Deserialize the arguments up front.
		let args = match decoder.decode::<types::COMPOUND4args>() {
			Ok(args) => args,
			Err(e) => {
				tracing::error!(?e, "Failed to decode COMPOUND args.");
				return rpc::error(None, rpc::ReplyAcceptedStat::GarbageArgs);
			},
		};

		// Handle verification.
		let verf = match self.handle_auth(cred, verf).await {
			Ok(verf) => verf,
			Err(stat) => return rpc::reject(rpc::ReplyRejected::AuthError(stat)),
		};

		// Handle the operations.
		let types::COMPOUND4args {
			tag,
			minorversion,
			argarray,
			..
		} = args;

		// Create the context
		let mut ctx = Context {
			minor_version: minorversion,
			current_file_handle: None,
			saved_file_handle: None,
		};

		let mut resarray = Vec::new(); // Result buffer.
		let mut status = nfsstat4::NFS4_OK; // Most recent status code.
		for arg in argarray {
			let opnum = arg.opnum();
			let result = tokio::time::timeout(
				std::time::Duration::from_secs(2),
				self.handle_arg(&mut ctx, arg.clone()),
			)
			.await
			.unwrap_or(nfs_resop4::Timeout(opnum));
			resarray.push(result.clone());
			if result.status() != nfsstat4::NFS4_OK {
				status = result.status();
				let is_lookup = matches!(opnum, nfs_opnum4::OP_LOOKUP | nfs_opnum4::OP_OPENATTR);
				let is_enoent = matches!(status, nfsstat4::NFS4ERR_NOENT);
				if matches!(status, nfsstat4::NFS4ERR_DELAY) {
					let node = self
						.get_node(ctx.current_file_handle.unwrap())
						.await
						.unwrap();
					let id: Option<tg::artifact::Id> = match &node.kind {
						NodeKind::File { file, .. } => file
							.id(self.inner.tg.as_ref())
							.await
							.ok()
							.cloned()
							.map(tg::artifact::Id::from),
						NodeKind::Directory { directory, .. } => directory
							.id(self.inner.tg.as_ref())
							.await
							.ok()
							.cloned()
							.map(tg::artifact::Id::from),
						NodeKind::Symlink { symlink, .. } => symlink
							.id(self.inner.tg.as_ref())
							.await
							.ok()
							.cloned()
							.map(tg::artifact::Id::from),
						_ => None,
					};
					tracing::error!(?id, ?opnum, "nfs response timed out");
				} else if !(is_lookup && is_enoent) {
					tracing::error!(?opnum, ?status, ?xid, "An error occurred.");
				}
				break;
			}
		}
		let results = COMPOUND4res {
			status,
			tag,
			resarray,
		};
		rpc::success(verf, results)
	}

	async fn handle_arg(&self, ctx: &mut Context, arg: nfs_argop4) -> nfs_resop4 {
		match arg {
			nfs_argop4::OP_ILLEGAL => nfs_resop4::OP_ILLEGAL(types::ILLEGAL4res {
				status: nfsstat4::NFS4ERR_OP_ILLEGAL,
			}),
			nfs_argop4::OP_ACCESS(arg) => nfs_resop4::OP_ACCESS(self.handle_access(ctx, arg).await),
			nfs_argop4::OP_CLOSE(arg) => nfs_resop4::OP_CLOSE(self.handle_close(ctx, arg).await),
			nfs_argop4::OP_COMMIT => nfs_resop4::OP_COMMIT,
			nfs_argop4::OP_CREATE => nfs_resop4::OP_CREATE,
			nfs_argop4::OP_DELEGPURGE => nfs_resop4::OP_DELEGPURGE,
			nfs_argop4::OP_DELEGRETURN => nfs_resop4::OP_DELEGRETURN,
			nfs_argop4::OP_GETATTR(arg) => {
				nfs_resop4::OP_GETATTR(self.handle_getattr(ctx, arg).await)
			},
			nfs_argop4::OP_GETFH => nfs_resop4::OP_GETFH(Self::handle_get_file_handle(ctx)),
			nfs_argop4::OP_LINK => nfs_resop4::OP_LINK,
			nfs_argop4::OP_LOCK(arg) => nfs_resop4::OP_LOCK(self.handle_lock(ctx, arg).await),
			nfs_argop4::OP_LOCKT(arg) => nfs_resop4::OP_LOCKT(self.handle_lockt(ctx, arg).await),
			nfs_argop4::OP_LOCKU(arg) => nfs_resop4::OP_LOCKU(self.handle_locku(ctx, arg).await),
			nfs_argop4::OP_LOOKUP(arg) => nfs_resop4::OP_LOOKUP(self.handle_lookup(ctx, arg).await),
			nfs_argop4::OP_LOOKUPP => nfs_resop4::OP_LOOKUPP(self.handle_lookup_parent(ctx).await),
			nfs_argop4::OP_NVERIFY(_) => nfs_resop4::OP_NVERIFY(NVERIFY4res {
				status: nfsstat4::NFS4ERR_NOTSUPP,
			}),
			nfs_argop4::OP_OPEN(arg) => nfs_resop4::OP_OPEN(self.handle_open(ctx, arg).await),
			nfs_argop4::OP_OPENATTR(arg) => {
				nfs_resop4::OP_OPENATTR(self.handle_openattr(ctx, arg).await)
			},
			nfs_argop4::OP_OPEN_CONFIRM(arg) => {
				nfs_resop4::OP_OPEN_CONFIRM(self.handle_open_confirm(ctx, arg).await)
			},
			nfs_argop4::OP_OPEN_DOWNGRADE => nfs_resop4::OP_OPEN_DOWNGRADE,
			nfs_argop4::OP_PUTFH(arg) => {
				nfs_resop4::OP_PUTFH(Self::handle_put_file_handle(ctx, &arg))
			},
			nfs_argop4::OP_PUTPUBFH => {
				Self::handle_put_file_handle(ctx, &PUTFH4args { object: ROOT });
				nfs_resop4::OP_PUTPUBFH(types::PUTPUBFH4res {
					status: nfsstat4::NFS4_OK,
				})
			},
			nfs_argop4::OP_PUTROOTFH => {
				Self::handle_put_file_handle(ctx, &PUTFH4args { object: ROOT });
				nfs_resop4::OP_PUTROOTFH(types::PUTROOTFH4res {
					status: nfsstat4::NFS4_OK,
				})
			},
			nfs_argop4::OP_READ(arg) => nfs_resop4::OP_READ(self.handle_read(ctx, arg).await),
			nfs_argop4::OP_READDIR(arg) => {
				nfs_resop4::OP_READDIR(self.handle_readdir(ctx, arg).await)
			},
			nfs_argop4::OP_READLINK => nfs_resop4::OP_READLINK(self.handle_readlink(ctx).await),
			nfs_argop4::OP_REMOVE => nfs_resop4::OP_REMOVE,
			nfs_argop4::OP_RENAME => nfs_resop4::OP_RENAME,
			nfs_argop4::OP_RENEW(arg) => nfs_resop4::OP_RENEW(self.handle_renew(arg)),
			nfs_argop4::OP_RESTOREFH => {
				nfs_resop4::OP_RESTOREFH(Self::handle_restore_file_handle(ctx))
			},
			nfs_argop4::OP_SAVEFH => nfs_resop4::OP_SAVEFH(Self::handle_save_file_handle(ctx)),
			nfs_argop4::OP_SECINFO(arg) => {
				nfs_resop4::OP_SECINFO(self.handle_sec_info(ctx, arg).await)
			},
			nfs_argop4::OP_SETATTR => nfs_resop4::OP_SETATTR,
			nfs_argop4::OP_SETCLIENTID(arg) => {
				nfs_resop4::OP_SETCLIENTID(self.handle_set_client_id(arg).await)
			},
			nfs_argop4::OP_SETCLIENTID_CONFIRM(arg) => {
				nfs_resop4::OP_SETCLIENTID_CONFIRM(self.handle_set_client_id_confirm(arg).await)
			},
			nfs_argop4::OP_VERIFY => nfs_resop4::OP_VERIFY,
			nfs_argop4::OP_WRITE => nfs_resop4::OP_WRITE,
			nfs_argop4::OP_RELEASE_LOCKOWNER(arg) => {
				nfs_resop4::OP_RELEASE_LOCKOWNER(self.handle_release_lockowner(ctx, arg).await)
			},
			nfs_argop4::Unimplemented(arg) => types::nfs_resop4::Unknown(arg),
		}
	}

	async fn get_node(&self, node: nfs_fh4) -> Option<Arc<Node>> {
		self.inner.nodes.read().await.get(&node.0).cloned()
	}

	async fn add_node(&self, id: u64, node: Arc<Node>) {
		self.inner.nodes.write().await.insert(id, node);
	}

	async fn add_client(&self, id: Vec<u8>, client: Arc<tokio::sync::RwLock<ClientData>>) {
		self.inner.clients.write().await.insert(id, client);
	}

	async fn get_client(
		&self,
		client: &nfs_client_id4,
	) -> Option<Arc<tokio::sync::RwLock<ClientData>>> {
		self.inner.clients.read().await.get(&client.id).cloned()
	}

	async fn add_lock(&self, id: u64, lock: Arc<tokio::sync::RwLock<LockState>>) {
		self.inner.locks.write().await.insert(id, lock);
	}

	async fn get_lock(&self, lock: u64) -> Option<Arc<tokio::sync::RwLock<LockState>>> {
		self.inner.locks.read().await.get(&lock).cloned()
	}

	async fn remove_lock(&self, lock: u64) {
		self.inner.locks.write().await.remove(&lock);
	}
}

impl Server {
	async fn handle_access(&self, ctx: &Context, arg: ACCESS4args) -> ACCESS4res {
		let Some(fh) = ctx.current_file_handle else {
			return ACCESS4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		let Some(node) = self.get_node(fh).await else {
			tracing::error!(?fh, "Unknown filehandle.");
			return ACCESS4res::Error(nfsstat4::NFS4ERR_BADHANDLE);
		};

		let access = match &node.kind {
			NodeKind::Root { .. }
			| NodeKind::Directory { .. }
			| NodeKind::NamedAttributeDirectory { .. } => ACCESS4_EXECUTE | ACCESS4_READ | ACCESS4_LOOKUP,
			NodeKind::Symlink { .. }
			| NodeKind::NamedAttribute { .. }
			| NodeKind::Checkout { .. } => ACCESS4_READ,
			NodeKind::File { file, .. } => {
				let is_executable = match file.executable(self.inner.tg.as_ref()).await {
					Ok(b) => b,
					Err(e) => {
						tracing::error!(?e, "Failed to lookup executable bit for file.");
						return ACCESS4res::Error(nfsstat4::NFS4ERR_IO);
					},
				};
				if is_executable {
					ACCESS4_EXECUTE | ACCESS4_READ
				} else {
					ACCESS4_READ
				}
			},
		};

		let supported = arg.access & access;
		let resok = ACCESS4resok { supported, access };

		ACCESS4res::NFS4_OK(resok)
	}

	async fn handle_close(&self, ctx: &Context, arg: CLOSE4args) -> CLOSE4res {
		let Some(fh) = ctx.current_file_handle else {
			return CLOSE4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		let Some(node) = self.get_node(fh).await else {
			tracing::error!(?fh, "Unknown filehandle.");
			return CLOSE4res::Error(nfsstat4::NFS4ERR_BADHANDLE);
		};

		let mut stateid = arg.open_stateid;

		if let NodeKind::File { .. } = &node.kind {
			// Look up the existing lock state.
			let index = stateid.index();
			let Some(lock_state) = self.get_lock(stateid.index()).await else {
				return CLOSE4res::Error(nfsstat4::NFS4ERR_BAD_STATEID);
			};
			let lock_state = lock_state.write().await;

			// Check if there are any outstanding byterange locks.
			if lock_state.byterange_locks.is_empty() {
				self.remove_lock(index).await;
			} else {
				return CLOSE4res::Error(nfsstat4::NFS4ERR_LOCKS_HELD);
			}
		}

		stateid.seqid = stateid.seqid.increment();
		CLOSE4res::NFS4_OK(stateid)
	}

	async fn handle_getattr(&self, ctx: &Context, arg: GETATTR4args) -> GETATTR4res {
		let Some(fh) = ctx.current_file_handle else {
			tracing::error!("Missing current file handle.");
			return GETATTR4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		match self.get_attr(fh, arg.attr_request).await {
			Ok(obj_attributes) => GETATTR4res::NFS4_OK(GETATTR4resok { obj_attributes }),
			Err(e) => GETATTR4res::Error(e),
		}
	}

	async fn get_attr(&self, file_handle: nfs_fh4, requested: bitmap4) -> Result<fattr4, nfsstat4> {
		if requested.0.is_empty() {
			return Ok(fattr4 {
				attrmask: bitmap4(Vec::default()),
				attr_vals: Vec::new(),
			});
		}

		let Some(data) = self.get_file_attr_data(file_handle).await else {
			tracing::error!(?file_handle, "Missing attr data.");
			return Err(nfsstat4::NFS4ERR_NOENT);
		};

		let attrmask = data.supported_attrs.intersection(&requested);
		let attr_vals = data.to_bytes(&attrmask);

		Ok(fattr4 {
			attrmask,
			attr_vals,
		})
	}

	#[allow(clippy::similar_names)]
	async fn get_file_attr_data(&self, file_handle: nfs_fh4) -> Option<FileAttrData> {
		let node = self.get_node(file_handle).await?;
		let data = match &node.kind {
			NodeKind::Root { .. } => FileAttrData::new(file_handle, nfs_ftype4::NF4DIR, 0, O_RX),
			NodeKind::Directory { children, .. } => {
				let len = children.read().await.len();
				FileAttrData::new(file_handle, nfs_ftype4::NF4DIR, len, O_RX)
			},
			NodeKind::File { file, size, .. } => {
				let is_executable = match file.executable(self.inner.tg.as_ref()).await {
					Ok(b) => b,
					Err(e) => {
						tracing::error!(?e, "Failed to lookup executable bit for file.");
						return None;
					},
				};
				let mode = if is_executable { O_RX } else { O_RDONLY };
				FileAttrData::new(
					file_handle,
					nfs_ftype4::NF4REG,
					size.to_usize().unwrap(),
					mode,
				)
			},
			NodeKind::Symlink { .. } | NodeKind::Checkout { .. } => {
				FileAttrData::new(file_handle, nfs_ftype4::NF4LNK, 1, O_RDONLY)
			},
			NodeKind::NamedAttribute { data } => {
				let len = data.len();
				FileAttrData::new(file_handle, nfs_ftype4::NF4NAMEDATTR, len, O_RDONLY)
			},
			NodeKind::NamedAttributeDirectory { children, .. } => {
				let len = children.read().await.len();
				FileAttrData::new(file_handle, nfs_ftype4::NF4ATTRDIR, len, O_RX)
			},
		};
		Some(data)
	}

	async fn handle_lock(&self, ctx: &mut Context, arg: LOCK4args) -> LOCK4res {
		// Required overflow check.
		if ![0, u64::MAX].contains(&arg.length) && (u64::MAX - arg.offset > arg.length) {
			return LOCK4res::Error(nfsstat4::NFS4ERR_INVAL);
		};

		// Since we're a read only file system we need to check if the client is attempting to acquire an exlusive (write) lock and return the appropriate error code.
		// NFS section 13.1.8.9 https://datatracker.ietf.org/doc/html/rfc7530#autoid-325
		match arg.locktype {
			nfs_lock_type4::WRITE_LT | nfs_lock_type4::WRITEW_LT => {
				return LOCK4res::Error(nfsstat4::NFS4ERR_OPENMODE);
			},
			_ => (),
		};

		// Get the arguments we care about.
		let range = (arg.offset, arg.length);
		let stateid = match arg.locker {
			locker4::TRUE(open_to_lock_owner) => open_to_lock_owner.open_stateid,
			locker4::FALSE(exist_lock_owner) => exist_lock_owner.lock_stateid,
		};

		// Lookup the lock state.
		let index = stateid.index();

		// We ignore returning an error here if the lock state does not exist to support erroneous CLOSE requests that clear out state.
		let lock_state = self.get_lock(index).await;
		if let Some(lock_state) = lock_state {
			// Add the new lock.
			let mut lock_state = lock_state.write().await;
			lock_state.byterange_locks.push(range);
		};

		// Return with the new stateid.
		let lock_stateid = stateid4::new(stateid.seqid.increment(), index, true);
		let resok = LOCK4resok { lock_stateid };
		LOCK4res::NFS4_OK(resok)
	}

	async fn handle_lockt(&self, ctx: &mut Context, arg: LOCKT4args) -> LOCKT4res {
		if ![0, u64::MAX].contains(&arg.length) && (u64::MAX - arg.offset > arg.length) {
			return LOCKT4res::Error(nfsstat4::NFS4ERR_INVAL);
		};
		match arg.locktype {
			nfs_lock_type4::WRITE_LT | nfs_lock_type4::WRITEW_LT => {
				return LOCKT4res::Error(nfsstat4::NFS4ERR_OPENMODE);
			},
			_ => (),
		};
		LOCKT4res::NFS4_OK
	}

	async fn handle_locku(&self, ctx: &mut Context, arg: LOCKU4args) -> LOCKU4res {
		let range = (arg.offset, arg.length);
		let mut lock_stateid = arg.lock_stateid;

		// Lookup the reader state.
		if let Some(lock_state) = self.get_lock(lock_stateid.index()).await {
			// Remove the lock.
			let mut lock_state = lock_state.write().await;
			let Some(index) = lock_state.byterange_locks.iter().position(|r| r == &range) else {
				return LOCKU4res::Error(nfsstat4::NFS4ERR_BAD_RANGE);
			};
			lock_state.byterange_locks.remove(index);
		};

		// Increment the seqid and return.
		lock_stateid.seqid = lock_stateid.seqid.increment();
		LOCKU4res::NFS4_OK(lock_stateid)
	}

	async fn handle_lookup(&self, ctx: &mut Context, arg: LOOKUP4args) -> LOOKUP4res {
		let Some(fh) = ctx.current_file_handle else {
			return LOOKUP4res {
				status: nfsstat4::NFS4ERR_NOFILEHANDLE,
			};
		};

		match self.lookup(fh, &arg.objname).await {
			Ok(Some(fh)) => {
				ctx.current_file_handle = Some(fh);
				LOOKUP4res {
					status: nfsstat4::NFS4_OK,
				}
			},
			Ok(None) => LOOKUP4res {
				status: nfsstat4::NFS4ERR_NOENT,
			},
			Err(status) => LOOKUP4res { status },
		}
	}

	async fn handle_lookup_parent(&self, ctx: &mut Context) -> LOOKUPP4res {
		let Some(fh) = ctx.current_file_handle else {
			return LOOKUPP4res {
				status: nfsstat4::NFS4ERR_NOFILEHANDLE,
			};
		};
		let Some(node) = self.get_node(fh).await else {
			return LOOKUPP4res {
				status: nfsstat4::NFS4ERR_BADHANDLE,
			};
		};
		let Some(parent) = node.parent.upgrade() else {
			return LOOKUPP4res {
				status: nfsstat4::NFS4ERR_IO,
			};
		};
		ctx.current_file_handle = Some(nfs_fh4(parent.id));
		LOOKUPP4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	async fn lookup(&self, parent: nfs_fh4, name: &str) -> Result<Option<nfs_fh4>, nfsstat4> {
		let parent_node = self.get_node(parent).await.ok_or(nfsstat4::NFS4ERR_NOENT)?;
		let Some(node) = self.get_or_create_child_node(parent_node, name).await? else {
			return Ok(None);
		};
		Ok(Some(nfs_fh4(node.id)))
	}

	#[allow(clippy::too_many_lines)]
	async fn get_or_create_child_node(
		&self,
		parent_node: Arc<Node>,
		name: &str,
	) -> Result<Option<Arc<Node>>, nfsstat4> {
		if name == "." {
			return Ok(Some(parent_node));
		}

		if name == ".." {
			let parent_parent_node = parent_node.parent.upgrade().ok_or(nfsstat4::NFS4ERR_IO)?;
			return Ok(Some(parent_parent_node));
		}

		match &parent_node.kind {
			NodeKind::Root { children, .. }
			| NodeKind::Directory { children, .. }
			| NodeKind::NamedAttributeDirectory { children, .. } => {
				if let Some(child) = children.read().await.get(name).cloned() {
					return Ok(Some(child));
				}
			},
			_ => {
				tracing::error!("Cannot create child on File or Symlink.");
				return Err(nfsstat4::NFS4ERR_NOTDIR);
			},
		}

		// Create the child data. This is either an artifact, or a named attribute value.
		let child_data = match &parent_node.kind {
			NodeKind::Root { .. } => {
				let id = name
					.parse::<tg::artifact::Id>()
					.map_err(|_| nfsstat4::NFS4ERR_NOENT)?;
				let path = Path::new("../checkouts").join(id.to_string());
				let exists = tokio::fs::symlink_metadata(self.inner.path.join(&path))
					.await
					.is_ok();
				if exists {
					Either::Left(Either::Left(path))
				} else {
					Either::Left(Either::Right(tg::Artifact::with_id(id)))
				}
			},

			NodeKind::Directory { directory, .. } => {
				let entries = directory
					.entries(self.inner.tg.as_ref())
					.await
					.map_err(|e| {
						tracing::error!(?e, ?name, "Failed to get directory entries.");
						nfsstat4::NFS4ERR_IO
					})?;
				let Some(entry) = entries.get(name) else {
					return Ok(None);
				};
				Either::Left(Either::Right(entry.clone()))
			},

			NodeKind::NamedAttributeDirectory { .. } => {
				// Currently, we only support one named attribute.
				if name != tg::file::TANGRAM_FILE_XATTR_NAME {
					return Ok(None);
				}
				let Some(grandparent_node) = parent_node.parent.upgrade() else {
					tracing::error!("Failed to upgrade parent node.");
					return Err(nfsstat4::NFS4ERR_IO);
				};
				// Currently, the only supported xattr refers to file references.
				let NodeKind::File { file, .. } = &grandparent_node.kind else {
					return Ok(None);
				};
				let file_references = match file.references(self.inner.tg.as_ref()).await {
					Ok(references) => references,
					Err(e) => {
						tracing::error!(?e, "Failed to get file references.");
						return Err(nfsstat4::NFS4ERR_IO);
					},
				};
				let mut references = Vec::new();
				for artifact in file_references {
					let id = artifact.id(self.inner.tg.as_ref()).await.map_err(|e| {
						tracing::error!(?e, ?artifact, "Failed to get artifact ID.");
						nfsstat4::NFS4ERR_IO
					})?;
					references.push(id);
				}
				let attributes = tg::file::Attributes { references };
				let data = serde_json::to_vec(&attributes).map_err(|e| {
					tracing::error!(?e, "Failed to serialize file attributes.");
					nfsstat4::NFS4ERR_IO
				})?;
				Either::Right(data)
			},
			_ => unreachable!(),
		};

		let node_id = self.next_node_id();
		let attributes = tokio::sync::RwLock::new(None);
		let kind = match child_data {
			Either::Left(Either::Left(path)) => NodeKind::Checkout { path },
			Either::Left(Either::Right(tg::Artifact::Directory(directory))) => {
				let children = tokio::sync::RwLock::new(Map::default());
				NodeKind::Directory {
					directory,
					children,
					attributes,
				}
			},
			Either::Left(Either::Right(tg::Artifact::File(file))) => {
				let size = file.size(self.inner.tg.as_ref()).await.map_err(|e| {
					tracing::error!(?e, "Failed to get size of file's contents.");
					nfsstat4::NFS4ERR_IO
				})?;
				NodeKind::File {
					file,
					size,
					attributes,
				}
			},
			Either::Left(Either::Right(tg::Artifact::Symlink(symlink))) => NodeKind::Symlink {
				symlink,
				attributes,
			},
			Either::Right(data) => NodeKind::NamedAttribute { data },
		};

		// Create the child node.
		let child_node = Node {
			id: node_id,
			parent: Arc::downgrade(&parent_node),
			kind,
		};
		let child_node = Arc::new(child_node);

		// Add the child node to the parent node.
		match &parent_node.kind {
			NodeKind::Root { children, .. }
			| NodeKind::Directory { children, .. }
			| NodeKind::NamedAttributeDirectory { children, .. } => {
				children
					.write()
					.await
					.insert(name.to_owned(), child_node.clone());
			},
			_ => unreachable!(),
		}

		// Add the child node to the nodes.
		self.add_node(node_id, child_node.clone()).await;
		Ok(Some(child_node))
	}

	async fn get_or_create_attributes_node(
		&self,
		parent_node: &Arc<Node>,
	) -> Result<Arc<Node>, nfsstat4> {
		match &parent_node.kind {
			NodeKind::Root { attributes, .. }
			| NodeKind::Directory { attributes, .. }
			| NodeKind::File { attributes, .. }
			| NodeKind::Symlink { attributes, .. } => {
				let mut attributes = attributes.write().await;
				if let Some(attributes) = attributes.as_ref() {
					return Ok(attributes.clone());
				};

				let id = self.next_node_id();
				let parent = Arc::downgrade(parent_node);
				let children = tokio::sync::RwLock::new(Map::default());
				let node = Node {
					id,
					parent,
					kind: NodeKind::NamedAttributeDirectory { children },
				};

				let node = Arc::new(node);
				attributes.replace(node.clone());
				self.add_node(id, node.clone()).await;
				Ok(node)
			},
			_ => Err(nfsstat4::NFS4ERR_NOTSUPP),
		}
	}

	fn next_node_id(&self) -> u64 {
		self.inner
			.node_index
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
	}

	fn next_client_id(&self) -> u64 {
		self.inner
			.client_index
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
	}

	async fn next_lock_id(&self) -> Option<u64> {
		// In the extremely unlikely event that the client has more files open then we can represent, return an error.
		let locks = self.inner.locks.read().await;
		if locks.len() == usize::MAX - 2 {
			tracing::error!("Failed to create the file reader.");
			return None;
		}

		// Find the next freely available lock.
		let mut lock_index = self.inner.lock_index.write().await;
		loop {
			let index = *lock_index;
			*lock_index += 1;
			if *lock_index == u64::MAX - 1 {
				*lock_index = 1;
			}
			if !locks.contains_key(&lock_index) {
				return Some(index);
			}
		}
	}

	async fn handle_open(&self, ctx: &mut Context, arg: OPEN4args) -> OPEN4res {
		let Some(fh) = ctx.current_file_handle else {
			return OPEN4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		// RFC 7530 16.15.5: If the underlying file system at the server is only accessible in a read-only mode and the OPEN request has specified OPEN4_SHARE_ACCESS_WRITE or OPEN4_SHARE_ACCESS_BOTH the server with return NFS4ERR_ROFS to indicate a read-only file system
		if (arg.share_access == OPEN4_SHARE_ACCESS_WRITE)
			|| (arg.share_access == OPEN4_SHARE_ACCESS_BOTH)
		{
			tracing::error!(?arg, "Share access violation.");
			return OPEN4res::Error(nfsstat4::NFS4ERR_ROFS);
		}

		let (fh, confirm_flags) = match arg.claim {
			open_claim4::CLAIM_NULL(name) => match self.lookup(fh, &name).await {
				Ok(Some(fh)) => (fh, OPEN4_RESULT_CONFIRM),
				Ok(None) => return OPEN4res::Error(nfsstat4::NFS4ERR_NOENT),
				Err(e) => return OPEN4res::Error(e),
			},
			open_claim4::CLAIM_PREVIOUS(open_delegation_type4::OPEN_DELEGATE_NONE) => (fh, 0),
			_ => {
				tracing::error!(?arg, "Unsupported open request.");
				return OPEN4res::Error(nfsstat4::NFS4ERR_NOTSUPP);
			},
		};

		ctx.current_file_handle = Some(fh);

		// Create the stateid.
		let Some(index) = self.next_lock_id().await else {
			return OPEN4res::Error(nfsstat4::NFS4ERR_IO);
		};

		let stateid = stateid4::new(arg.seqid, index, false);

		if let NodeKind::File { file, .. } = &self.get_node(fh).await.unwrap().kind {
			let Ok(reader) = file.reader(self.inner.tg.as_ref()).await else {
				tracing::error!("Failed to create the file reader.");
				return OPEN4res::Error(nfsstat4::NFS4ERR_IO);
			};

			// Create a new lock state.
			let byterange_locks = Vec::new();
			let lock_state = LockState {
				reader: Some(reader),
				fh,
				byterange_locks,
			};

			self.add_lock(index, Arc::new(tokio::sync::RwLock::new(lock_state)))
				.await;
		}

		let cinfo = change_info4 {
			atomic: false,
			before: 0,
			after: 0,
		};

		let rflags = confirm_flags | OPEN4_RESULT_LOCKTYPE_POSIX;
		let attrset = bitmap4(vec![]);
		let delegation = open_delegation4::OPEN_DELEGATE_NONE;
		let resok = OPEN4resok {
			stateid,
			cinfo,
			rflags,
			attrset,
			delegation,
		};
		OPEN4res::NFS4_OK(resok)
	}

	async fn handle_openattr(&self, ctx: &mut Context, arg: OPENATTR4args) -> OPENATTR4res {
		if arg.createdir {
			return OPENATTR4res {
				status: nfsstat4::NFS4ERR_PERM,
			};
		}
		let Some(fh) = ctx.current_file_handle else {
			return OPENATTR4res {
				status: nfsstat4::NFS4ERR_NOFILEHANDLE,
			};
		};
		let Some(node) = self.get_node(fh).await else {
			return OPENATTR4res {
				status: nfsstat4::NFS4ERR_BADHANDLE,
			};
		};
		let NodeKind::File { file, .. } = &node.kind else {
			return OPENATTR4res {
				status: nfsstat4::NFS4ERR_NOENT,
			};
		};
		if file
			.references(self.inner.tg.as_ref())
			.await
			.map_or(true, <[tg::Artifact]>::is_empty)
		{
			return OPENATTR4res {
				status: nfsstat4::NFS4ERR_NOENT,
			};
		}
		let attributes_node = match self.get_or_create_attributes_node(&node).await {
			Ok(node) => node,
			Err(status) => return OPENATTR4res { status },
		};
		ctx.current_file_handle = Some(nfs_fh4(attributes_node.id));
		OPENATTR4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	async fn handle_open_confirm(
		&self,
		ctx: &mut Context,
		arg: OPEN_CONFIRM4args,
	) -> OPEN_CONFIRM4res {
		if arg.seqid != arg.open_stateid.seqid.increment() {
			tracing::error!(?arg, "Invalid seqid in open.");
			self.remove_lock(arg.open_stateid.index()).await;
			return OPEN_CONFIRM4res::Error(nfsstat4::NFS4ERR_BAD_SEQID);
		}
		let mut open_stateid = arg.open_stateid;
		open_stateid.seqid = arg.seqid;
		OPEN_CONFIRM4res::NFS4_OK(OPEN_CONFIRM4resok { open_stateid })
	}

	async fn handle_read(&self, ctx: &Context, arg: READ4args) -> READ4res {
		let Some(fh) = ctx.current_file_handle else {
			return READ4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};
		let Some(node) = self.get_node(fh).await else {
			tracing::error!(?fh, "Unknown filehandle.");
			return READ4res::Error(nfsstat4::NFS4ERR_BADHANDLE);
		};

		// RFC 7530 16.23.4:
		// "If the current file handle is not a regular file, an error will be returned to the client. In the case where the current filehandle represents a directory, NFS4ERR_ISDIR is returned; otherwise, NFS4ERR_INVAL is returned."
		let (file, file_size) = match &node.kind {
			NodeKind::File { file, size, .. } => (file, size),
			NodeKind::Directory { .. } | NodeKind::Root { .. } => {
				return READ4res::Error(nfsstat4::NFS4ERR_ISDIR)
			},
			// Special case: named attributes (xattrs) are not stored in regular files in our NFS implementation, so we handle them specially here.
			NodeKind::NamedAttribute { data } => {
				// RFC 7530 5.3
				// Once an OPEN is done, named attributes may be examined and changed by normal READ and WRITE operations using the filehandles and stateids returned by OPEN
				let len = data.len().min(arg.count.to_usize().unwrap());
				let offset = arg.offset.to_usize().unwrap().min(len);
				let data = data[offset..len].to_vec();
				let eof = (offset + len) == data.len();
				let res = READ4resok { eof, data };
				return READ4res::NFS4_OK(res);
			},
			_ => return READ4res::Error(nfsstat4::NFS4ERR_INVAL),
		};

		// It is allowed for clients to attempt to read past the end of a file, in which case the server returns an empty file.
		if arg.offset >= *file_size {
			return READ4res::NFS4_OK(READ4resok {
				eof: true,
				data: vec![],
			});
		}

		let read_size = arg
			.count
			.to_u64()
			.unwrap()
			.min(file_size - arg.offset)
			.to_usize()
			.unwrap();

		// Check if the lock state exists.
		let lock_state = self.get_lock(arg.stateid.index()).await;

		// RFC 7530: If a stateid value is used that has all zeros or all ones in the "other" field but does not match one of the cases above, the server MUST return the error NFS4ERR_BAD_STATEID.
		// https://datatracker.ietf.org/doc/html/rfc7530#section-9.1.4.3
		if !arg.stateid.is_valid() {
			tracing::error!(?arg, "Invalid stateid.");
			return READ4res::Error(nfsstat4::NFS4ERR_BAD_STATEID);
		}

		// This fallback exists for special state ids and any erroneous read.
		let (data, eof) = if [ANONYMOUS_STATE_ID, READ_BYPASS_STATE_ID].contains(&arg.stateid)
			|| !lock_state.is_some()
		{
			// We need to create a reader just for this request.
			let Ok(mut reader) = file.reader(self.inner.tg.as_ref()).await else {
				tracing::error!("Failed to create the file reader.");
				return READ4res::Error(nfsstat4::NFS4ERR_IO);
			};
			if let Err(e) = reader.seek(std::io::SeekFrom::Start(arg.offset)).await {
				tracing::error!(?e, "Failed to seek.");
				return READ4res::Error(e.into());
			}
			let mut data = vec![0u8; read_size];
			if let Err(e) = reader.read_exact(&mut data).await {
				tracing::error!(?e, "Failed to read from the file.");
				return READ4res::Error(e.into());
			}
			let eof = (arg.offset + arg.count.to_u64().unwrap()) >= *file_size;
			(data, eof)
		} else {
			let lock_state = lock_state.unwrap();
			let mut lock_state = lock_state.write().await;
			if lock_state.fh != fh {
				tracing::error!(?fh, ?arg.stateid, "Reader registered for wrong file id. file: {file}.");
				return READ4res::Error(nfsstat4::NFS4ERR_BAD_STATEID);
			}
			let reader = lock_state.reader.as_mut().unwrap();
			if let Err(e) = reader.seek(std::io::SeekFrom::Start(arg.offset)).await {
				tracing::error!(?e, "Failed to seek.");
				return READ4res::Error(e.into());
			}
			let mut data = vec![0; read_size];
			if let Err(e) = reader.read_exact(&mut data).await {
				tracing::error!(?e, "Failed to read.");
				return READ4res::Error(e.into());
			}

			let eof = (arg.offset + arg.count.to_u64().unwrap()) >= *file_size;
			(data, eof)
		};
		READ4res::NFS4_OK(READ4resok { eof, data })
	}

	async fn handle_readdir(&self, ctx: &Context, arg: READDIR4args) -> READDIR4res {
		let Some(fh) = ctx.current_file_handle else {
			return READDIR4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		let Some(node) = self.get_node(fh).await else {
			return READDIR4res::Error(nfsstat4::NFS4ERR_BADHANDLE);
		};

		let cookie = arg.cookie.to_usize().unwrap();
		let mut count = 0;

		let entries = match &node.kind {
			NodeKind::Root { .. } => Vec::default(),
			NodeKind::Directory { directory, .. } => {
				let Ok(entries) = directory.entries(self.inner.tg.as_ref()).await else {
					return READDIR4res::Error(nfsstat4::NFS4ERR_IO);
				};
				entries.keys().cloned().collect::<Vec<_>>()
			},
			NodeKind::NamedAttributeDirectory { .. } => {
				vec![tg::file::TANGRAM_FILE_XATTR_NAME.into()]
			},
			_ => return READDIR4res::Error(nfsstat4::NFS4ERR_NOTDIR),
		};

		let mut reply = Vec::with_capacity(entries.len());
		let names = entries.iter().map(AsRef::as_ref);

		let mut eof = true;
		for (cookie, name) in [".", ".."]
			.into_iter()
			.chain(names)
			.enumerate()
			.skip(cookie)
		{
			let node = match name {
				"." => node.clone(),
				".." => node.parent.upgrade().unwrap(),
				_ => match self.get_or_create_child_node(node.clone(), name).await {
					Ok(Some(node)) => node,
					Ok(None) => return READDIR4res::Error(nfsstat4::NFS4ERR_NOENT),
					Err(e) => return READDIR4res::Error(e),
				},
			};
			let attrs = self
				.get_attr(nfs_fh4(node.id), arg.attr_request.clone())
				.await
				.unwrap();
			let cookie = cookie.to_u64().unwrap();
			let name = name.to_owned();

			// Size of the cookie + size of the attr + size of the name
			count += std::mem::size_of_val(&cookie); // u64
			count += 4 + 4 * attrs.attrmask.0.len(); // bitmap4
			count += 4 + attrs.attr_vals.len(); // opaque<>
			count += 4 + name.len(); // utf8_cstr

			if count > arg.dircount.to_usize().unwrap() {
				eof = false;
				break;
			}

			let entry = entry4 {
				cookie,
				name,
				attrs,
			};
			reply.push(entry);
		}

		let cookieverf = fh.0.to_be_bytes();
		let reply = dirlist4 {
			entries: reply,
			eof,
		};
		READDIR4res::NFS4_OK(READDIR4resok { cookieverf, reply })
	}

	async fn handle_readlink(&self, ctx: &Context) -> READLINK4res {
		let Some(fh) = ctx.current_file_handle else {
			return READLINK4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};
		let Some(node) = self.get_node(fh).await else {
			return READLINK4res::Error(nfsstat4::NFS4ERR_NOENT);
		};
		if let NodeKind::Checkout { path } = &node.kind {
			let link = path.as_os_str().as_bytes().to_owned();
			return READLINK4res::NFS4_OK(READLINK4resok { link });
		}
		let NodeKind::Symlink { symlink, .. } = &node.kind else {
			return READLINK4res::Error(nfsstat4::NFS4ERR_INVAL);
		};
		let mut target = String::new();
		let Ok(artifact) = symlink.artifact(self.inner.tg.as_ref()).await else {
			return READLINK4res::Error(nfsstat4::NFS4ERR_IO);
		};
		let Ok(path) = symlink.path(self.inner.tg.as_ref()).await else {
			return READLINK4res::Error(nfsstat4::NFS4ERR_IO);
		};
		if let Some(artifact) = artifact {
			let Ok(id) = artifact.id(self.inner.tg.as_ref()).await else {
				return READLINK4res::Error(nfsstat4::NFS4ERR_IO);
			};
			for _ in 0..node.depth() - 1 {
				target.push_str("../");
			}
			target.push_str(&id.to_string());
		}
		if artifact.is_some() && path.is_some() {
			target.push('/');
		}
		if let Some(path) = path {
			target.push_str(path);
		}
		READLINK4res::NFS4_OK(READLINK4resok {
			link: target.into_bytes(),
		})
	}

	fn handle_renew(&self, arg: RENEW4args) -> RENEW4res {
		RENEW4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	async fn handle_sec_info(&self, ctx: &Context, arg: SECINFO4args) -> SECINFO4res {
		let Some(parent) = ctx.current_file_handle else {
			return SECINFO4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};
		match self.lookup(parent, &arg.name).await {
			Ok(_) => SECINFO4res::NFS4_OK(vec![]),
			Err(e) => SECINFO4res::Error(e),
		}
	}

	async fn handle_set_client_id(&self, arg: SETCLIENTID4args) -> SETCLIENTID4res {
		let Some(client) = self.get_client(&arg.client).await else {
			let server_id = self.next_client_id();
			let server_verifier = [0; NFS4_VERIFIER_SIZE];
			let record = ClientData {
				server_id,
				client_verifier: arg.client.verifier,
				server_verifier,
				callback: arg.callback,
				callback_ident: arg.callback_ident,
				confirmed: false,
			};
			self.add_client(arg.client.id, Arc::new(tokio::sync::RwLock::new(record)))
				.await;
			return SETCLIENTID4res::NFS4_OK(SETCLIENTID4resok {
				clientid: server_id,
				setclientid_confirm: server_verifier,
			});
		};

		let client = client.read().await;
		let conditions = [
			client.confirmed,
			client.client_verifier == arg.client.verifier,
			client.callback == arg.callback,
			client.callback_ident == arg.callback_ident,
		];

		if conditions.into_iter().all(|c| c) {
			let clientid = client.server_id;
			let setclientid_confirm = client.server_verifier;
			SETCLIENTID4res::NFS4_OK(SETCLIENTID4resok {
				clientid,
				setclientid_confirm,
			})
		} else {
			tracing::error!(?conditions, "Failed to set client id.");
			SETCLIENTID4res::Error(nfsstat4::NFS4ERR_IO)
		}
	}

	async fn handle_set_client_id_confirm(
		&self,
		arg: SETCLIENTID_CONFIRM4args,
	) -> SETCLIENTID_CONFIRM4res {
		let clients = self.inner.clients.read().await;
		for client in clients.values() {
			let mut client = client.write().await;
			if client.server_id == arg.clientid {
				if client.server_verifier != arg.setclientid_confirm {
					return SETCLIENTID_CONFIRM4res {
						status: nfsstat4::NFS4ERR_CLID_INUSE,
					};
				}
				client.confirmed = true;
				return SETCLIENTID_CONFIRM4res {
					status: nfsstat4::NFS4_OK,
				};
			}
		}
		return SETCLIENTID_CONFIRM4res {
			status: nfsstat4::NFS4ERR_STALE_CLIENTID,
		};
	}

	async fn handle_release_lockowner(
		&self,
		context: &mut Context,
		arg: RELEASE_LOCKOWNER4args,
	) -> RELEASE_LOCKOWNER4res {
		RELEASE_LOCKOWNER4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	fn handle_put_file_handle(ctx: &mut Context, arg: &PUTFH4args) -> PUTFH4res {
		ctx.current_file_handle = Some(arg.object);
		PUTFH4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	fn handle_get_file_handle(ctx: &Context) -> GETFH4res {
		if let Some(object) = ctx.current_file_handle {
			GETFH4res::NFS4_OK(GETFH4resok { object })
		} else {
			GETFH4res::Error(nfsstat4::NFS4ERR_BADHANDLE)
		}
	}

	fn handle_save_file_handle(ctx: &mut Context) -> SAVEFH4res {
		ctx.saved_file_handle = ctx.current_file_handle;
		SAVEFH4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	fn handle_restore_file_handle(ctx: &mut Context) -> RESTOREFH4res {
		ctx.current_file_handle = ctx.saved_file_handle.take();
		RESTOREFH4res {
			status: nfsstat4::NFS4_OK,
		}
	}
}

impl Node {
	fn depth(self: &Arc<Self>) -> usize {
		if self.id == 0 {
			0
		} else {
			self.parent.upgrade().unwrap().depth() + 1
		}
	}
}

pub const O_RDONLY: u32 = MODE4_RUSR | MODE4_RGRP | MODE4_ROTH;
pub const O_RX: u32 = MODE4_XUSR | MODE4_XGRP | MODE4_XOTH | O_RDONLY;

pub const ALL_SUPPORTED_ATTRS: &[u32] = &[
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
	FATTR4_TIME_BACKUP,
	FATTR4_TIME_CREATE,
	FATTR4_TIME_DELTA,
	FATTR4_TIME_METADATA,
	FATTR4_TIME_MODIFY,
	FATTR4_MOUNTED_ON_FILEID,
];

#[allow(clippy::struct_excessive_bools)]
pub struct FileAttrData {
	supported_attrs: bitmap4,
	file_type: nfs_ftype4,
	expire_type: u32,
	change: u64,
	size: u64,
	link_support: bool,
	symlink_support: bool,
	named_attr: bool,
	fsid: fsid4,
	unique_handles: bool,
	lease_time: u32,
	rdattr_error: i32,
	file_handle: nfs_fh4,
	acl: Vec<nfsace4>,
	aclsupport: u32,
	archive: bool,
	cansettime: bool,
	case_insensitive: bool,
	case_preserving: bool,
	chown_restricted: bool,
	fileid: u64,
	files_avail: u64,
	files_free: u64,
	files_total: u64,
	fs_locations: fs_locations4,
	hidden: bool,
	homogeneous: bool,
	maxfilesize: u64,
	maxlink: u32,
	maxname: u32,
	maxread: u64,
	maxwrite: u64,
	mimetype: Vec<String>,
	mode: u32,
	no_trunc: bool,
	numlinks: u32,
	owner: String,
	owner_group: String,
	quota_avail_hard: u64,
	quota_avail_soft: u64,
	quota_used: u64,
	rawdev: specdata4,
	space_avail: u64,
	space_free: u64,
	space_total: u64,
	space_used: u64,
	system: bool,
	time_access: nfstime4,
	time_backup: nfstime4,
	time_create: nfstime4,
	time_delta: nfstime4,
	time_metadata: nfstime4,
	time_modify: nfstime4,
	mounted_on_fileid: u64,
}

impl FileAttrData {
	fn new(file_handle: nfs_fh4, file_type: nfs_ftype4, size: usize, mode: u32) -> FileAttrData {
		let size = size.to_u64().unwrap();
		let mut supported_attrs = bitmap4(Vec::new());
		for attr in ALL_SUPPORTED_ATTRS {
			supported_attrs.set(attr.to_usize().unwrap());
		}
		let change = nfstime4::now().seconds.to_u64().unwrap();
		// Note: The "named_attr" attribute represents whether the named attribute directory is non-empty, not whether or not it exists.
		let named_attr = matches!(file_type, nfs_ftype4::NF4REG);
		FileAttrData {
			supported_attrs,
			file_type,
			expire_type: 0,
			change,
			size,
			link_support: true,
			symlink_support: true,
			named_attr,
			fsid: fsid4 { major: 0, minor: 1 },
			unique_handles: true,
			lease_time: 1000,
			rdattr_error: 0,
			file_handle,
			acl: Vec::new(),
			aclsupport: 0,
			archive: true,
			cansettime: false,
			case_insensitive: false,
			case_preserving: true,
			chown_restricted: true,
			fileid: file_handle.0,
			files_avail: 0,
			files_free: 0,
			files_total: 1,
			hidden: false,
			homogeneous: true,
			maxfilesize: u64::MAX,
			maxlink: u32::MAX,
			maxname: 512,
			maxread: u64::MAX,
			maxwrite: 0,
			mimetype: Vec::new(),
			mode,
			fs_locations: fs_locations4 {
				fs_root: pathname4(vec!["/".into()]),
				locations: Vec::new(),
			},
			no_trunc: true,
			numlinks: 1,
			owner: "tangram@tangram".to_owned(),
			owner_group: "tangram@tangram".to_owned(),
			quota_avail_hard: 0,
			quota_avail_soft: 0,
			quota_used: 0,
			rawdev: specdata4 {
				specdata1: 0,
				specdata2: 0,
			},
			space_avail: 0,
			space_free: 0,
			space_total: u64::MAX,
			space_used: size.to_u64().unwrap(),
			system: false,
			time_access: nfstime4::new(),
			time_backup: nfstime4::new(),
			time_create: nfstime4::new(),
			time_delta: nfstime4::new(),
			time_metadata: nfstime4::new(),
			time_modify: nfstime4::new(),
			mounted_on_fileid: file_handle.0,
		}
	}

	fn to_bytes(&self, requested: &bitmap4) -> Vec<u8> {
		let mut buf = Vec::with_capacity(256);
		let mut encoder = xdr::Encoder::new(&mut buf);
		for attr in ALL_SUPPORTED_ATTRS.iter().copied() {
			if !requested.get(attr.to_usize().unwrap()) {
				continue;
			}
			match attr {
				FATTR4_SUPPORTED_ATTRS => encoder.encode(&self.supported_attrs.0).unwrap(),
				FATTR4_TYPE => encoder.encode(&self.file_type).unwrap(),
				FATTR4_FH_EXPIRE_TYPE => encoder.encode(&self.expire_type).unwrap(),
				FATTR4_CHANGE => encoder.encode(&self.change).unwrap(),
				FATTR4_SIZE => encoder.encode(&self.size).unwrap(),
				FATTR4_LINK_SUPPORT => encoder.encode(&self.link_support).unwrap(),
				FATTR4_SYMLINK_SUPPORT => encoder.encode(&self.symlink_support).unwrap(),
				FATTR4_NAMED_ATTR => encoder.encode(&self.named_attr).unwrap(),
				FATTR4_FSID => encoder.encode(&self.fsid).unwrap(),
				FATTR4_UNIQUE_HANDLES => encoder.encode(&self.unique_handles).unwrap(),
				FATTR4_LEASE_TIME => encoder.encode(&self.lease_time).unwrap(),
				FATTR4_RDATTR_ERROR => encoder.encode(&self.rdattr_error).unwrap(),
				FATTR4_FILEHANDLE => encoder.encode(&self.file_handle).unwrap(),
				FATTR4_ACL => encoder.encode(&self.acl).unwrap(),
				FATTR4_ACLSUPPORT => encoder.encode(&self.aclsupport).unwrap(),
				FATTR4_ARCHIVE => encoder.encode(&self.archive).unwrap(),
				FATTR4_CANSETTIME => encoder.encode(&self.cansettime).unwrap(),
				FATTR4_CASE_INSENSITIVE => encoder.encode(&self.case_insensitive).unwrap(),
				FATTR4_CASE_PRESERVING => encoder.encode(&self.case_preserving).unwrap(),
				FATTR4_CHOWN_RESTRICTED => encoder.encode(&self.chown_restricted).unwrap(),
				FATTR4_FILEID => encoder.encode(&self.fileid).unwrap(),
				FATTR4_FILES_AVAIL => encoder.encode(&self.files_avail).unwrap(),
				FATTR4_FILES_FREE => encoder.encode(&self.files_free).unwrap(),
				FATTR4_FILES_TOTAL => encoder.encode(&self.files_total).unwrap(),
				FATTR4_HIDDEN => encoder.encode(&self.hidden).unwrap(),
				FATTR4_HOMOGENEOUS => encoder.encode(&self.homogeneous).unwrap(),
				FATTR4_MAXFILESIZE => encoder.encode(&self.maxfilesize).unwrap(),
				FATTR4_MAXLINK => encoder.encode(&self.maxlink).unwrap(),
				FATTR4_MAXNAME => encoder.encode(&self.maxname).unwrap(),
				FATTR4_MAXREAD => encoder.encode(&self.maxread).unwrap(),
				FATTR4_MAXWRITE => encoder.encode(&self.maxwrite).unwrap(),
				FATTR4_MIMETYPE => encoder.encode(&self.mimetype).unwrap(),
				FATTR4_MODE => encoder.encode(&self.mode).unwrap(),
				FATTR4_FS_LOCATIONS => encoder.encode(&self.fs_locations).unwrap(),
				FATTR4_NO_TRUNC => encoder.encode(&self.no_trunc).unwrap(),
				FATTR4_NUMLINKS => encoder.encode(&self.numlinks).unwrap(),
				FATTR4_OWNER => encoder.encode(&self.owner).unwrap(),
				FATTR4_OWNER_GROUP => encoder.encode(&self.owner_group).unwrap(),
				FATTR4_QUOTA_AVAIL_HARD => encoder.encode(&self.quota_avail_hard).unwrap(),
				FATTR4_QUOTA_AVAIL_SOFT => encoder.encode(&self.quota_avail_soft).unwrap(),
				FATTR4_QUOTA_USED => encoder.encode(&self.quota_used).unwrap(),
				FATTR4_RAWDEV => encoder.encode(&self.rawdev).unwrap(),
				FATTR4_SPACE_AVAIL => encoder.encode(&self.space_avail).unwrap(),
				FATTR4_SPACE_FREE => encoder.encode(&self.space_free).unwrap(),
				FATTR4_SPACE_TOTAL => encoder.encode(&self.space_total).unwrap(),
				FATTR4_SPACE_USED => encoder.encode(&self.space_used).unwrap(),
				FATTR4_SYSTEM => encoder.encode(&self.system).unwrap(),
				FATTR4_TIME_ACCESS => encoder.encode(&self.time_access).unwrap(),
				FATTR4_TIME_BACKUP => encoder.encode(&self.time_backup).unwrap(),
				FATTR4_TIME_CREATE => encoder.encode(&self.time_create).unwrap(),
				FATTR4_TIME_DELTA => encoder.encode(&self.time_delta).unwrap(),
				FATTR4_TIME_METADATA => encoder.encode(&self.time_metadata).unwrap(),
				FATTR4_TIME_MODIFY => encoder.encode(&self.time_modify).unwrap(),
				FATTR4_MOUNTED_ON_FILEID => encoder.encode(&self.mounted_on_fileid).unwrap(),
				_ => (),
			};
		}
		buf
	}
}
