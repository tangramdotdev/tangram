use self::{
	provider::{ExtAttr, Provider},
	types::{
		ACCESS4_EXECUTE, ACCESS4_LOOKUP, ACCESS4_READ, ACCESS4args, ACCESS4res, ACCESS4resok,
		ANONYMOUS_STATE_ID, CLOSE4args, CLOSE4res, COMPOUND4args, COMPOUND4res, FATTR4_ACL,
		FATTR4_ACLSUPPORT, FATTR4_ARCHIVE, FATTR4_CANSETTIME, FATTR4_CASE_INSENSITIVE,
		FATTR4_CASE_PRESERVING, FATTR4_CHANGE, FATTR4_CHOWN_RESTRICTED, FATTR4_FH_EXPIRE_TYPE,
		FATTR4_FILEHANDLE, FATTR4_FILEID, FATTR4_FILES_AVAIL, FATTR4_FILES_FREE,
		FATTR4_FILES_TOTAL, FATTR4_FS_LOCATIONS, FATTR4_FSID, FATTR4_HIDDEN, FATTR4_HOMOGENEOUS,
		FATTR4_LEASE_TIME, FATTR4_LINK_SUPPORT, FATTR4_MAXFILESIZE, FATTR4_MAXLINK, FATTR4_MAXNAME,
		FATTR4_MAXREAD, FATTR4_MAXWRITE, FATTR4_MIMETYPE, FATTR4_MODE, FATTR4_MOUNTED_ON_FILEID,
		FATTR4_NAMED_ATTR, FATTR4_NO_TRUNC, FATTR4_NUMLINKS, FATTR4_OWNER, FATTR4_OWNER_GROUP,
		FATTR4_QUOTA_AVAIL_HARD, FATTR4_QUOTA_AVAIL_SOFT, FATTR4_QUOTA_USED, FATTR4_RAWDEV,
		FATTR4_RDATTR_ERROR, FATTR4_SIZE, FATTR4_SPACE_AVAIL, FATTR4_SPACE_FREE,
		FATTR4_SPACE_TOTAL, FATTR4_SPACE_USED, FATTR4_SUPPORTED_ATTRS, FATTR4_SYMLINK_SUPPORT,
		FATTR4_SYSTEM, FATTR4_TIME_ACCESS, FATTR4_TIME_BACKUP, FATTR4_TIME_CREATE,
		FATTR4_TIME_DELTA, FATTR4_TIME_METADATA, FATTR4_TIME_MODIFY, FATTR4_TYPE,
		FATTR4_UNIQUE_HANDLES, GETATTR4args, GETATTR4res, GETATTR4resok, GETFH4res, GETFH4resok,
		ILLEGAL4res, LOCK4args, LOCK4res, LOCK4resok, LOCKT4args, LOCKT4res, LOCKU4args, LOCKU4res,
		LOOKUP4args, LOOKUP4res, LOOKUPP4res, MODE4_RGRP, MODE4_ROTH, MODE4_RUSR, MODE4_XGRP,
		MODE4_XOTH, MODE4_XUSR, NFS_PROG, NFS_VERS, NFS4_VERIFIER_SIZE, NVERIFY4res,
		OPEN_CONFIRM4args, OPEN_CONFIRM4res, OPEN_CONFIRM4resok, OPEN4_RESULT_CONFIRM,
		OPEN4_RESULT_LOCKTYPE_POSIX, OPEN4_SHARE_ACCESS_BOTH, OPEN4_SHARE_ACCESS_WRITE, OPEN4args,
		OPEN4res, OPEN4resok, OPENATTR4args, OPENATTR4res, PUTFH4args, PUTFH4res, PUTPUBFH4res,
		PUTROOTFH4res, READ_BYPASS_STATE_ID, READ4args, READ4res, READ4resok, READDIR4args,
		READDIR4res, READDIR4resok, READLINK4res, READLINK4resok, RELEASE_LOCKOWNER4args,
		RELEASE_LOCKOWNER4res, RENEW4args, RENEW4res, RESTOREFH4res, RPC_VERS, SAVEFH4res,
		SECINFO4args, SECINFO4res, SETCLIENTID_CONFIRM4args, SETCLIENTID_CONFIRM4res,
		SETCLIENTID4args, SETCLIENTID4res, SETCLIENTID4resok, bitmap4, cb_client4, change_info4,
		dirlist4, entry4, fattr4, fs_locations4, fsid4, locker4, nfs_argop4, nfs_fh4, nfs_ftype4,
		nfs_lock_type4, nfs_opnum4, nfs_resop4, nfsace4, nfsstat4, nfstime4, open_claim4,
		open_delegation_type4, open_delegation4, pathname4, specdata4, stateid4, verifier4,
	},
};
use crate::{Attrs, FileType, Provider as _};
use dashmap::DashMap;
use futures::{TryFutureExt as _, future};
use num::ToPrimitive as _;
use std::{
	io::Error,
	path::{Path, PathBuf},
	pin::pin,
	sync::{Arc, Mutex, atomic::AtomicU64},
	time::Duration,
};
use tangram_futures::task::{Stop, Task};
use tokio::net::{TcpListener, TcpStream};

mod provider;

pub mod rpc;
pub mod types;
pub mod xdr;

const ROOT: nfs_fh4 = nfs_fh4(crate::ROOT_NODE_ID);

pub struct Server<P>(Arc<Inner<P>>);

pub struct Inner<P> {
	client_index: AtomicU64,
	clients: DashMap<Vec<u8>, Arc<tokio::sync::RwLock<ClientData>>>,
	host: String,
	path: PathBuf,
	port: u16,
	provider: Provider<P>,
	task: Mutex<Option<Task<()>>>,
}

struct ClientData {
	callback: cb_client4,
	callback_ident: u32,
	client_verifier: verifier4,
	confirmed: bool,
	server_id: u64,
	server_verifier: verifier4,
}

#[derive(Clone, Debug)]
struct Context {
	current_file_handle: Option<nfs_fh4>,
	#[allow(dead_code)]
	minor_version: u32,
	saved_file_handle: Option<nfs_fh4>,
}

impl<P> Server<P>
where
	P: crate::Provider + Send + Sync + 'static,
{
	pub async fn start(
		provider: P,
		path: &Path,
		host: &str,
		port: u16,
	) -> Result<Self, std::io::Error> {
		// Create the server.
		let provider = Provider::new(provider);
		let server = Self(Arc::new(Inner {
			client_index: AtomicU64::new(0),
			clients: DashMap::default(),
			host: host.to_owned(),
			path: path.to_owned(),
			port,
			provider,
			task: Mutex::new(None),
		}));

		// Listen.
		let addr = format!("{host}:{port}");
		let listener = TcpListener::bind(&addr).await?;

		// Unmount.
		unmount(&server.path).await.ok();

		// Mount.
		Self::mount(&server.path, &server.host, server.port).await?;

		// Spawn the task.
		let request_handler_task = Task::spawn(|stop| {
			let server = server.clone();
			async move {
				server
					.request_handler_task(listener, stop)
					.await
					.inspect_err(|error| {
						tracing::error!(?error);
					})
					.ok();
			}
		});

		let shutdown = async move {
			request_handler_task.stop();
			request_handler_task.wait().await.unwrap();
		};

		// Spawn the task.
		let task = Task::spawn(|stop| async move {
			stop.wait().await;
			shutdown.await;
		});
		server.task.lock().unwrap().replace(task);

		Ok(server)
	}

	pub fn stop(&self) {
		self.task.lock().unwrap().as_ref().unwrap().stop();
	}

	pub async fn wait(&self) {
		let task = self.task.lock().unwrap().clone().unwrap();
		task.wait().await.unwrap();
	}

	async fn request_handler_task(
		&self,
		listener: TcpListener,
		stop: Stop,
	) -> Result<(), std::io::Error> {
		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

		loop {
			// Accept.
			let accept = listener.accept();
			let stop = stop.clone();
			let (stream, _addr) = match future::select(pin!(accept), pin!(stop.wait())).await {
				future::Either::Left((result, _)) => match result {
					Ok(stream) => stream,
					Err(error) => {
						tracing::error!(?error, "failed to accept a connection");
						continue;
					},
				},
				future::Either::Right(((), _)) => {
					break;
				},
			};

			// Spawn a task to handle the connection.
			task_tracker.spawn({
				let server = self.clone();
				async move {
					server
						.handle_connection(stream, stop)
						.await
						.inspect_err(|error| {
							tracing::error!(?error);
						})
						.ok();
				}
			});
		}

		// Wait for all tasks to complete.
		task_tracker.close();
		task_tracker.wait().await;

		// Unmount.
		unmount(&self.path)
			.await
			.inspect_err(|error| {
				tracing::error!(?error, "failed to unmount");
			})
			.ok();

		Ok(())
	}

	async fn mount(path: &Path, host: &str, port: u16) -> Result<(), std::io::Error> {
		let options = format!(
			"async,actimeo=60,mutejukebox,noacl,noquota,nobrowse,rdonly,rsize=2097152,nocallback,tcp,vers=4,namedattr,port={port}"
		);
		let url = format!("{host}:/");
		let status = tokio::process::Command::new("mount_nfs")
			.arg("-o")
			.arg(options)
			.arg(url)
			.arg(path)
			.stdout(std::process::Stdio::null())
			.stderr(std::process::Stdio::null())
			.status()
			.await?;
		if !status.success() {
			return Err(Error::other("failed to mount"));
		}
		Ok(())
	}

	async fn handle_connection(&self, stream: TcpStream, stop: Stop) -> Result<(), std::io::Error> {
		let (mut reader, mut writer) = tokio::io::split(stream);

		// Create the task tracker.
		let task_tracker = tokio_util::task::TaskTracker::new();

		// Create the writer task.
		let (message_sender, mut message_receiver) =
			tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
		task_tracker.spawn(async move {
			while let Some(message) = message_receiver.recv().await {
				rpc::write_fragments(&mut writer, &message)
					.await
					.inspect_err(|error| tracing::error!(%error))
					.ok();
			}
		});

		// Receive incoming message fragments.
		loop {
			let read = rpc::read_fragments(&mut reader);
			let fragments = match future::select(pin!(read), pin!(stop.wait())).await {
				future::Either::Left((result, _)) => result?,
				future::Either::Right(((), _)) => {
					break;
				},
			};

			task_tracker.spawn({
				let server = self.clone();
				let message_sender = message_sender.clone();
				async move {
					let mut decoder = xdr::Decoder::from_bytes(&fragments);
					let mut buffer = Vec::with_capacity(4096);
					let mut encoder = xdr::Encoder::new(&mut buffer);
					while let Ok(message) = decoder.decode::<rpc::Message>() {
						let xid = message.xid;
						let Some(body) = server.handle_message(message, &mut decoder).await else {
							continue;
						};
						let body = rpc::MessageBody::Reply(body);
						let message = rpc::Message { xid, body };
						encoder.encode(&message).unwrap();
					}
					message_sender.send(buffer).unwrap();
				}
			});
		}

		// Drop the message sender to avoid deadlocking the writer task.
		drop(message_sender);

		// Wait for all tasks to complete.
		task_tracker.close();
		task_tracker.wait().await;

		Ok(())
	}

	async fn handle_message(
		&self,
		message: rpc::Message,
		decoder: &mut xdr::Decoder<'_>,
	) -> Option<rpc::ReplyBody> {
		match message.clone().body {
			rpc::MessageBody::Call(call) => {
				if call.rpcvers != RPC_VERS {
					tracing::error!(?call, "version mismatch");
					let rejected = rpc::ReplyRejected::RpcMismatch {
						low: RPC_VERS,
						high: RPC_VERS,
					};
					let body = rpc::ReplyBody::Rejected(rejected);
					return Some(body);
				}

				if call.vers != NFS_VERS {
					tracing::error!(?call, "program mismatch");
					return Some(rpc::error(None, rpc::ReplyAcceptedStat::ProgramMismatch {
						low: NFS_VERS,
						high: NFS_VERS,
					}));
				}

				if call.prog != NFS_PROG {
					tracing::error!(?call, "expected NFS4_PROGRAM but got {}", call.prog);
					return Some(rpc::error(None, rpc::ReplyAcceptedStat::ProgramUnavailable));
				}

				let reply = match call.proc {
					0 => Self::handle_null(),
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
	#[allow(clippy::unnecessary_wraps)]
	async fn handle_auth(
		&self,
		_cred: rpc::Auth,
		_verf: rpc::Auth,
	) -> std::result::Result<Option<rpc::Auth>, rpc::AuthStat> {
		Ok(None)
	}

	fn handle_null() -> rpc::ReplyBody {
		rpc::success(None, ())
	}

	// See <https://datatracker.ietf.org/doc/html/rfc7530#section-17.2>.
	async fn handle_compound(
		&self,
		xid: u32,
		cred: rpc::Auth,
		verf: rpc::Auth,
		decoder: &mut xdr::Decoder<'_>,
	) -> rpc::ReplyBody {
		// Deserialize the arguments up front.
		let args = match decoder.decode::<COMPOUND4args>() {
			Ok(args) => args,
			Err(e) => {
				tracing::error!(?e, "failed to decode COMPOUND args");
				return rpc::error(None, rpc::ReplyAcceptedStat::GarbageArgs);
			},
		};

		// Handle verification.
		let verf = match self.handle_auth(cred, verf).await {
			Ok(verf) => verf,
			Err(stat) => return rpc::reject(rpc::ReplyRejected::AuthError(stat)),
		};

		let COMPOUND4args {
			tag,
			minorversion,
			argarray,
			..
		} = args;

		// Create the context.
		let mut ctx = Context {
			minor_version: minorversion,
			current_file_handle: None,
			saved_file_handle: None,
		};

		let mut resarray = Vec::new();
		let mut status = nfsstat4::NFS4_OK;
		for arg in argarray {
			let opnum = arg.opnum();
			let result = tokio::time::timeout(
				Duration::from_secs(2),
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
					tracing::error!(?ctx, ?opnum, "nfs response timed out");
				} else if !(is_lookup && is_enoent) {
					tracing::error!(?ctx, ?opnum, ?status, ?xid, "an error occurred");
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
			nfs_argop4::OP_ILLEGAL => nfs_resop4::OP_ILLEGAL(ILLEGAL4res {
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
				nfs_resop4::OP_PUTPUBFH(PUTPUBFH4res {
					status: nfsstat4::NFS4_OK,
				})
			},
			nfs_argop4::OP_PUTROOTFH => {
				Self::handle_put_file_handle(ctx, &PUTFH4args { object: ROOT });
				nfs_resop4::OP_PUTROOTFH(PUTROOTFH4res {
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
			nfs_argop4::OP_RENEW(arg) => nfs_resop4::OP_RENEW(Self::handle_renew(arg)),
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
			nfs_argop4::Unimplemented(arg) => nfs_resop4::Unknown(arg),
		}
	}
}

impl<P> Server<P>
where
	P: crate::Provider + Send + Sync + 'static,
{
	async fn handle_access(&self, ctx: &Context, arg: ACCESS4args) -> ACCESS4res {
		let Some(fh) = ctx.current_file_handle else {
			return ACCESS4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		let attr = match self.provider.get_attr_ext(fh.0).await {
			Ok(attr) => attr,
			Err(error) => return ACCESS4res::Error(error.into()),
		};
		let access = match attr {
			ExtAttr::Normal(Attrs {
				typ: FileType::Directory,
				..
			})
			| ExtAttr::AttrDir(_) => ACCESS4_EXECUTE | ACCESS4_READ | ACCESS4_LOOKUP,
			ExtAttr::Normal(Attrs {
				typ: FileType::Symlink,
				..
			})
			| ExtAttr::AttrFile(_) => ACCESS4_READ,
			ExtAttr::Normal(Attrs {
				typ: FileType::File { executable, .. },
				..
			}) => {
				if executable {
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
		let Some(_fh) = ctx.current_file_handle else {
			return CLOSE4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};
		let mut stateid = arg.open_stateid;
		self.provider.close(stateid.index()).await;
		stateid.seqid = stateid.seqid.increment();
		CLOSE4res::NFS4_OK(stateid)
	}

	async fn handle_getattr(&self, ctx: &Context, arg: GETATTR4args) -> GETATTR4res {
		let Some(fh) = ctx.current_file_handle else {
			tracing::error!("missing current file handle");
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
			tracing::error!(?file_handle, "missing attr data");
			return Err(nfsstat4::NFS4ERR_NOENT);
		};

		let attrmask = data.supported_attrs.intersection(&requested);
		let attr_vals = data.to_bytes(&attrmask);

		Ok(fattr4 {
			attrmask,
			attr_vals,
		})
	}

	async fn get_file_attr_data(&self, file_handle: nfs_fh4) -> Option<FileAttrData> {
		if file_handle == ROOT {
			return Some(FileAttrData::new(file_handle, nfs_ftype4::NF4DIR, 0, O_RX));
		}

		let attr = self.provider.get_attr_ext(file_handle.0).await.ok()?;
		let data = match attr {
			ExtAttr::Normal(Attrs {
				typ: FileType::Directory,
				..
			}) => {
				let handle = self.provider.opendir(file_handle.0).await.ok()?;
				let children = self.provider.readdir(handle).await.ok()?;
				let len = children.len();
				FileAttrData::new(file_handle, nfs_ftype4::NF4DIR, len, O_RX)
			},
			ExtAttr::Normal(Attrs {
				typ: FileType::File { size, executable },
				..
			}) => {
				let mode = if executable { O_RX } else { O_RDONLY };
				FileAttrData::new(
					file_handle,
					nfs_ftype4::NF4REG,
					size.to_usize().unwrap(),
					mode,
				)
			},
			ExtAttr::Normal(Attrs {
				typ: FileType::Symlink,
				..
			}) => FileAttrData::new(file_handle, nfs_ftype4::NF4LNK, 1, O_RDONLY),
			ExtAttr::AttrDir(len) => {
				FileAttrData::new(file_handle, nfs_ftype4::NF4ATTRDIR, len, O_RX)
			},
			ExtAttr::AttrFile(len) => {
				FileAttrData::new(file_handle, nfs_ftype4::NF4NAMEDATTR, len, O_RDONLY)
			},
		};

		Some(data)
	}

	async fn handle_lock(&self, _ctx: &mut Context, arg: LOCK4args) -> LOCK4res {
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
		let stateid = match arg.locker {
			locker4::TRUE(open_to_lock_owner) => open_to_lock_owner.open_stateid,
			locker4::FALSE(exist_lock_owner) => exist_lock_owner.lock_stateid,
		};

		// Lookup the lock state.
		let index = stateid.index();

		// Return with the new stateid.
		let lock_stateid = stateid4::new(stateid.seqid.increment(), index, true);
		let resok = LOCK4resok { lock_stateid };
		LOCK4res::NFS4_OK(resok)
	}

	async fn handle_lockt(&self, _ctx: &mut Context, arg: LOCKT4args) -> LOCKT4res {
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

	async fn handle_locku(&self, _ctx: &mut Context, arg: LOCKU4args) -> LOCKU4res {
		let mut lock_stateid = arg.lock_stateid;

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
		let Ok(parent) = self.provider.lookup_parent(fh.0).await else {
			return LOOKUPP4res {
				status: nfsstat4::NFS4ERR_BADHANDLE,
			};
		};
		ctx.current_file_handle = Some(nfs_fh4(parent));
		LOOKUPP4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	async fn lookup(&self, parent: nfs_fh4, name: &str) -> Result<Option<nfs_fh4>, nfsstat4> {
		self.provider
			.lookup(parent.0, name)
			.map_ok(|id| id.map(nfs_fh4))
			.map_err(nfsstat4::from)
			.await
	}

	fn next_client_id(&self) -> u64 {
		self.client_index
			.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
	}

	async fn handle_open(&self, ctx: &mut Context, arg: OPEN4args) -> OPEN4res {
		let Some(fh) = ctx.current_file_handle else {
			return OPEN4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		// RFC 7530 16.15.5: If the underlying file system at the server is only accessible in a read-only mode and the OPEN request has specified OPEN4_SHARE_ACCESS_WRITE or OPEN4_SHARE_ACCESS_BOTH the server with return NFS4ERR_ROFS to indicate a read-only file system
		if (arg.share_access == OPEN4_SHARE_ACCESS_WRITE)
			|| (arg.share_access == OPEN4_SHARE_ACCESS_BOTH)
		{
			tracing::error!(?arg, "share access violation");
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
				tracing::error!(?arg, "unsupported open request");
				return OPEN4res::Error(nfsstat4::NFS4ERR_NOTSUPP);
			},
		};

		ctx.current_file_handle = Some(fh);

		// Open the file and create the state id.
		let Ok(handle) = self.provider.open(fh.0).await else {
			return OPEN4res::Error(nfsstat4::NFS4ERR_IO);
		};
		let stateid = stateid4::new(arg.seqid, handle, false);

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
		let attr_node = match self.provider.get_attr_dir(fh.0).await {
			Ok(attr) => attr,
			Err(error) => {
				return OPENATTR4res {
					status: error.into(),
				};
			},
		};
		ctx.current_file_handle = Some(nfs_fh4(attr_node));
		OPENATTR4res {
			status: nfsstat4::NFS4_OK,
		}
	}

	async fn handle_open_confirm(
		&self,
		_ctx: &mut Context,
		arg: OPEN_CONFIRM4args,
	) -> OPEN_CONFIRM4res {
		if arg.seqid != arg.open_stateid.seqid.increment() {
			tracing::error!(?arg, "invalid seqid in open");
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
		let Ok(attr) = self.provider.get_attr_ext(fh.0).await else {
			return READ4res::Error(nfsstat4::NFS4ERR_BADHANDLE);
		};

		// RFC 7530: If a stateid value is used that has all zeros or all ones in the "other" field but does not match one of the cases above, the server MUST return the error NFS4ERR_BAD_STATEID.
		// https://datatracker.ietf.org/doc/html/rfc7530#section-9.1.4.3
		if !arg.stateid.is_valid() {
			tracing::error!(?arg, "invalid stateid");
			return READ4res::Error(nfsstat4::NFS4ERR_BAD_STATEID);
		}

		// RFC 7530 16.23.4:
		// "if the current file handle is not a regular file, an error will be returned to the client. In the case where the current filehandle represents a directory, NFS4ERR_ISDIR is returned; otherwise, NFS4ERR_INVAL is returned"
		let size = match &attr {
			ExtAttr::Normal(Attrs {
				typ: FileType::File { size, .. },
				..
			}) => *size,
			ExtAttr::Normal(Attrs {
				typ: FileType::Directory,
				..
			})
			| ExtAttr::AttrDir(_) => return READ4res::Error(nfsstat4::NFS4ERR_ISDIR),
			ExtAttr::Normal(Attrs {
				typ: FileType::Symlink,
				..
			}) => return READ4res::Error(nfsstat4::NFS4ERR_INVAL),
			ExtAttr::AttrFile(len) => len.to_u64().unwrap(),
		};

		// It is allowed for clients to attempt to read past the end of a file, in which case the server returns an empty file.
		if arg.offset >= size {
			return READ4res::NFS4_OK(READ4resok {
				eof: true,
				data: vec![],
			});
		}

		// Compute the size of the read.
		let read_size = arg
			.count
			.to_u64()
			.unwrap()
			.min(size - arg.offset)
			.to_usize()
			.unwrap();

		// This fallback exists for special state ids that indicate a file has not been opened.
		let (data, eof) = if [ANONYMOUS_STATE_ID, READ_BYPASS_STATE_ID].contains(&arg.stateid) {
			let Ok(read_handle) = self.provider.open(fh.0).await else {
				return READ4res::Error(nfsstat4::NFS4ERR_IO);
			};
			let Ok(bytes) = self
				.provider
				.read(read_handle, arg.offset, read_size.to_u64().unwrap())
				.await
			else {
				return READ4res::Error(nfsstat4::NFS4ERR_IO);
			};
			self.provider.close(read_handle).await;
			let eof = (arg.offset + arg.count.to_u64().unwrap()) >= size;
			(bytes.to_vec(), eof)
		} else {
			let read_handle = arg.stateid.index();
			let Ok(bytes) = self
				.provider
				.read(read_handle, arg.offset, read_size.to_u64().unwrap())
				.await
			else {
				return READ4res::Error(nfsstat4::NFS4ERR_IO);
			};
			let eof = (arg.offset + arg.count.to_u64().unwrap()) >= size;
			(bytes.to_vec(), eof)
		};

		READ4res::NFS4_OK(READ4resok { eof, data })
	}

	async fn handle_readdir(&self, ctx: &Context, arg: READDIR4args) -> READDIR4res {
		let Some(fh) = ctx.current_file_handle else {
			return READDIR4res::Error(nfsstat4::NFS4ERR_NOFILEHANDLE);
		};

		let mut handle = u64::from_be_bytes(arg.cookieverf);
		if handle == 0 {
			handle = match self.provider.opendir(fh.0).await {
				Ok(handle) => handle,
				Err(error) => return READDIR4res::Error(error.into()),
			};
		}

		let entries = match self.provider.readdir(handle).await {
			Ok(entries) => entries,
			Err(error) => return READDIR4res::Error(error.into()),
		};

		let mut reply = Vec::with_capacity(entries.len());
		let mut count = 0;
		let mut eof = true;
		for (cookie, (name, id)) in entries
			.into_iter()
			.enumerate()
			.skip(arg.cookie.to_usize().unwrap())
		{
			let attrs = self
				.get_attr(nfs_fh4(id), arg.attr_request.clone())
				.await
				.unwrap();
			let cookie = cookie.to_u64().unwrap();

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
		let cookieverf = handle.to_be_bytes();
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

		match self.provider.readlink(fh.0).await {
			Ok(link) => READLINK4res::NFS4_OK(READLINK4resok {
				link: link.to_vec(),
			}),
			Err(error) => READLINK4res::Error(error.into()),
		}
	}

	fn handle_renew(_arg: RENEW4args) -> RENEW4res {
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
		let client = self
			.clients
			.get(&arg.client.id)
			.map(|client| client.clone());
		let Some(client) = client else {
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

			self.clients
				.insert(arg.client.id, Arc::new(tokio::sync::RwLock::new(record)));

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
			tracing::error!(?conditions, "failed to set client id");
			SETCLIENTID4res::Error(nfsstat4::NFS4ERR_IO)
		}
	}

	async fn handle_set_client_id_confirm(
		&self,
		arg: SETCLIENTID_CONFIRM4args,
	) -> SETCLIENTID_CONFIRM4res {
		for client in &self.clients {
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
		SETCLIENTID_CONFIRM4res {
			status: nfsstat4::NFS4ERR_STALE_CLIENTID,
		}
	}

	async fn handle_release_lockowner(
		&self,
		_context: &mut Context,
		_arg: RELEASE_LOCKOWNER4args,
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

impl<P> Clone for Server<P> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}

impl<P> std::ops::Deref for Server<P> {
	type Target = Inner<P>;

	fn deref(&self) -> &Self::Target {
		&self.0
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

pub async fn unmount(path: &Path) -> Result<(), std::io::Error> {
	tokio::process::Command::new("umount")
		.args(["-f"])
		.arg(path)
		.stdout(std::process::Stdio::null())
		.stderr(std::process::Stdio::null())
		.status()
		.await?;
	Ok(())
}
