use super::*;

enum Plan {
	Local,
	Provider(ProviderRequest),
}

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn handle_request_sync_batch(
		&self,
		fd: &OwnedFd,
		requests: &[PendingRequest],
		batch_results: &mut Vec<Dispatch>,
	) -> Result<()> {
		enum BatchAction {
			Deferred,
			NeedsProvider,
			Ready(Result<Response>),
		}

		// Plan the request batch.
		let mut actions = Vec::with_capacity(requests.len());
		let mut provider_requests = Vec::<ProviderRequest>::with_capacity(requests.len());

		for pending_request in requests {
			let request = &pending_request.request;
			let action = match self.plan_request(request) {
				Err(error) => BatchAction::Ready(Err(error)),
				Ok(Plan::Local) => BatchAction::Ready(self.handle_local_request_sync(fd, request)),
				Ok(Plan::Provider(provider_request)) => {
					provider_requests.push(provider_request);
					BatchAction::NeedsProvider
				},
			};
			actions.push(action);
		}

		// Execute the synchronous provider batch.
		if !provider_requests.is_empty() {
			let mut provider_results = self
				.provider
				.handle_batch_sync(provider_requests)
				.into_iter();
			let provider_count = actions
				.iter()
				.filter(|action| matches!(action, BatchAction::NeedsProvider))
				.count();
			if provider_results.len() != provider_count {
				return Err(Error::other("mismatched provider batch response length"));
			}
			for (action, pending_request) in std::iter::zip(actions.iter_mut(), requests) {
				if let BatchAction::NeedsProvider = action {
					let provider_result = provider_results
						.next()
						.ok_or_else(|| Error::other("missing provider batch response"))?;
					let next_action = match provider_result {
						Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => {
							BatchAction::Deferred
						},
						Err(error) => BatchAction::Ready(Err(error)),
						Ok(response) => {
							let result = self.map_provider_response_sync(
								fd,
								&pending_request.request,
								response,
							);
							BatchAction::Ready(result)
						},
					};
					*action = next_action;
				}
			}
		}

		// Assemble the dispatch results.
		batch_results.clear();
		batch_results.reserve(requests.len());
		for (action, request) in std::iter::zip(actions, requests) {
			match action {
				BatchAction::Deferred => batch_results.push(Dispatch::Deferred),
				BatchAction::NeedsProvider => {
					return Err(Error::other("missing provider batch result"));
				},
				BatchAction::Ready(result) => {
					let deferred = result.as_ref().is_err_and(|error| {
						error.raw_os_error() == Some(libc::ENOSYS)
							&& request.request.header.opcode != sys::fuse_opcode_FUSE_OPENDIR
					});
					if deferred {
						batch_results.push(Dispatch::Deferred);
					} else {
						self.register_response_resources(
							fd,
							request.request.header.unique,
							&result,
						)?;
						batch_results.push(Dispatch::Ready(result));
					}
				},
			}
		}
		Ok(())
	}

	pub(super) async fn handle_request(&self, fd: &OwnedFd, request: Request) -> Result<Response> {
		// Dispatch the request.
		let unique = request.header.unique;
		let result = match self.plan_request(&request) {
			Err(error) => Err(error),
			Ok(Plan::Local) => self.handle_local_request(fd, &request).await,
			Ok(Plan::Provider(provider_request)) => {
				let mut results = self.provider.handle_batch(vec![provider_request]).await;
				if results.len() != 1 {
					return Err(Error::other("mismatched provider batch response length"));
				}
				match results.pop().unwrap() {
					Err(error) => Err(error),
					Ok(response) => {
						let handle = Self::provider_response_handle(&response);
						let result = self.map_provider_response(fd, &request, response);
						if result.is_err()
							&& let Some(handle) = handle
						{
							self.provider.close(handle).await;
						}
						result
					},
				}
			},
		};

		// Track resources until the response is committed.
		self.register_response_resources(fd, unique, &result)?;

		result
	}

	fn plan_request(&self, request: &Request) -> Result<Plan> {
		// Select the provider or local execution path.
		let provider_request = match &request.data {
			RequestData::GetAttr | RequestData::Statx(_) => ProviderRequest::GetAttr {
				id: request.header.nodeid,
			},
			RequestData::GetXattr(_, name) => {
				let name = name
					.to_str()
					.map_err(|_| Error::from_raw_os_error(libc::ENODATA))?
					.to_owned();
				ProviderRequest::GetXattr {
					id: request.header.nodeid,
					name,
				}
			},
			RequestData::ListXattr(_) => ProviderRequest::ListXattrs {
				id: request.header.nodeid,
			},
			RequestData::Lookup(name) => {
				let name = name
					.to_str()
					.map_err(|_| Error::from_raw_os_error(libc::ENOENT))?
					.to_owned();
				ProviderRequest::LookupAndRemember {
					id: request.header.nodeid,
					name,
				}
			},
			RequestData::Open(data) => {
				Self::validate_open_request(*data)?;
				ProviderRequest::Open {
					id: request.header.nodeid,
				}
			},
			RequestData::OpenDir(data) => {
				Self::validate_open_request(*data)?;
				if self.no_opendir_support {
					return Err(Error::from_raw_os_error(libc::ENOSYS));
				}
				ProviderRequest::OpenDir {
					id: request.header.nodeid,
				}
			},
			RequestData::Read(data) => ProviderRequest::Read {
				handle: data.fh,
				length: data.size.to_u64().unwrap(),
				position: data.offset,
			},
			RequestData::ReadDir(data) if !self.no_opendir_support => ProviderRequest::ReadDir {
				handle: data.fh,
				length: data.size.to_u64().unwrap(),
				offset: data.offset,
			},
			RequestData::ReadDirPlus(data) if !self.no_opendir_support => {
				ProviderRequest::ReadDirPlus {
					handle: data.fh,
					length: data.size.to_u64().unwrap(),
					offset: data.offset,
				}
			},
			RequestData::ReadLink => ProviderRequest::ReadLink {
				id: request.header.nodeid,
			},
			RequestData::Init(_) => return Err(Error::other("unexpected init request")),
			RequestData::BatchForget(..)
			| RequestData::Destroy
			| RequestData::Flush
			| RequestData::Forget(_)
			| RequestData::Interrupt(_)
			| RequestData::ReadDir(_)
			| RequestData::ReadDirPlus(_)
			| RequestData::Release(_)
			| RequestData::ReleaseDir(_)
			| RequestData::Statfs
			| RequestData::Unsupported(_) => return Ok(Plan::Local),
		};

		Ok(Plan::Provider(provider_request))
	}

	pub(super) fn validate_open_request(request: fuse_open_in) -> Result<()> {
		let access_mode = request.flags & libc::O_ACCMODE.to_u32().unwrap();
		if access_mode != libc::O_RDONLY.to_u32().unwrap() {
			return Err(Error::from_raw_os_error(libc::EROFS));
		}

		Ok(())
	}

	fn handle_local_request_sync(&self, fd: &OwnedFd, request: &Request) -> Result<Response> {
		// Execute the local request synchronously.
		let response = match &request.data {
			RequestData::BatchForget(data, entries) => {
				let count = data.count.to_usize().unwrap_or(0);
				for entry in entries.iter().take(count) {
					self.provider.forget_sync(entry.nodeid, entry.nlookup);
				}
				Response::BatchForget
			},
			RequestData::Destroy => Response::Destroy,
			RequestData::Flush => Response::Flush,
			RequestData::Forget(data) => {
				self.provider
					.forget_sync(request.header.nodeid, data.nlookup);
				Response::Forget
			},
			RequestData::Interrupt(data) => return self.handle_interrupt_request(*data),
			RequestData::ReadDir(data) => {
				let entries = self.provider.readdir_node_sync(
					request.header.nodeid,
					data.offset,
					data.size.to_u64().unwrap(),
				)?;
				Self::build_read_dir_response(entries, *data)
			},
			RequestData::ReadDirPlus(data) => {
				let entries = self.provider.readdirplus_node_sync(
					request.header.nodeid,
					data.offset,
					data.size.to_u64().unwrap(),
				)?;
				self.build_read_dir_plus_response(entries, *data)
			},
			RequestData::Release(data) => {
				self.close_passthrough_backing(fd, data.fh);
				self.provider.close_sync(data.fh);
				Response::Release
			},
			RequestData::ReleaseDir(data) => {
				if !self.no_opendir_support {
					self.close_passthrough_backing(fd, data.fh);
					self.provider.close_sync(data.fh);
				}
				Response::ReleaseDir
			},
			RequestData::Statfs => Self::statfs_response(),
			RequestData::Unsupported(opcode) => {
				return Self::handle_unsupported_request(request.header, *opcode);
			},
			RequestData::GetAttr
			| RequestData::GetXattr(..)
			| RequestData::Init(_)
			| RequestData::ListXattr(_)
			| RequestData::Lookup(_)
			| RequestData::Open(_)
			| RequestData::OpenDir(_)
			| RequestData::Read(_)
			| RequestData::ReadLink
			| RequestData::Statx(_) => return Err(Error::other("unexpected local request")),
		};

		Ok(response)
	}

	async fn handle_local_request(&self, fd: &OwnedFd, request: &Request) -> Result<Response> {
		// Execute the local request asynchronously.
		let response = match &request.data {
			RequestData::BatchForget(data, entries) => {
				let requests = entries
					.iter()
					.take(data.count.to_usize().unwrap_or(0))
					.map(|entry| ProviderRequest::Forget {
						id: entry.nodeid,
						nlookup: entry.nlookup,
					})
					.collect::<Vec<_>>();
				if !requests.is_empty() {
					let _ = self.provider.handle_batch(requests).await;
				}
				Response::BatchForget
			},
			RequestData::Destroy => Response::Destroy,
			RequestData::Flush => Response::Flush,
			RequestData::Forget(data) => {
				let _ = self
					.provider
					.handle_batch(vec![ProviderRequest::Forget {
						id: request.header.nodeid,
						nlookup: data.nlookup,
					}])
					.await;
				Response::Forget
			},
			RequestData::Interrupt(data) => return self.handle_interrupt_request(*data),
			RequestData::ReadDir(data) => {
				let entries = self
					.provider
					.readdir_node(
						request.header.nodeid,
						data.offset,
						data.size.to_u64().unwrap(),
					)
					.await?;
				Self::build_read_dir_response(entries, *data)
			},
			RequestData::ReadDirPlus(data) => {
				let entries = self
					.provider
					.readdirplus_node(
						request.header.nodeid,
						data.offset,
						data.size.to_u64().unwrap(),
					)
					.await?;
				self.build_read_dir_plus_response(entries, *data)
			},
			RequestData::Release(data) => {
				self.close_passthrough_backing(fd, data.fh);
				self.provider.close(data.fh).await;
				Response::Release
			},
			RequestData::ReleaseDir(data) => {
				if !self.no_opendir_support {
					self.close_passthrough_backing(fd, data.fh);
					self.provider.close(data.fh).await;
				}
				Response::ReleaseDir
			},
			RequestData::Statfs => Self::statfs_response(),
			RequestData::Unsupported(opcode) => {
				return Self::handle_unsupported_request(request.header, *opcode);
			},
			RequestData::GetAttr
			| RequestData::GetXattr(..)
			| RequestData::Init(_)
			| RequestData::ListXattr(_)
			| RequestData::Lookup(_)
			| RequestData::Open(_)
			| RequestData::OpenDir(_)
			| RequestData::Read(_)
			| RequestData::ReadLink
			| RequestData::Statx(_) => return Err(Error::other("unexpected local request")),
		};

		Ok(response)
	}

	pub(super) fn map_provider_response_sync(
		&self,
		fd: &OwnedFd,
		request: &Request,
		response: ProviderResponse,
	) -> Result<Response> {
		let handle = Self::provider_response_handle(&response);
		let result = self.map_provider_response(fd, request, response);
		if result.is_err()
			&& let Some(handle) = handle
		{
			self.provider.close_sync(handle);
		}

		result
	}

	#[must_use]
	fn provider_response_handle(response: &ProviderResponse) -> Option<u64> {
		match response {
			ProviderResponse::Open { handle, .. } | ProviderResponse::OpenDir { handle } => {
				Some(*handle)
			},
			_ => None,
		}
	}

	fn map_provider_response(
		&self,
		fd: &OwnedFd,
		request: &Request,
		response: ProviderResponse,
	) -> Result<Response> {
		// Validate and translate the provider response.
		match &request.data {
			RequestData::GetAttr => {
				let ProviderResponse::GetAttr { attrs } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let out = Self::fuse_attr_out(request.header.nodeid, attrs);
				Ok(Response::GetAttr(out))
			},
			RequestData::GetXattr(request, _) => {
				let ProviderResponse::GetXattr { value } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let attr = value
					.map(|value| value.to_vec())
					.ok_or_else(|| Error::from_raw_os_error(libc::ENODATA))?;
				if request.size == 0 {
					let response = fuse_getxattr_out {
						padding: 0,
						size: attr.len().to_u32().unwrap(),
					};
					Ok(Response::GetXattr(response.as_bytes().to_vec()))
				} else if request.size.to_usize().unwrap() < attr.len() {
					Err(Error::from_raw_os_error(libc::ERANGE))
				} else {
					Ok(Response::GetXattr(attr))
				}
			},
			RequestData::ListXattr(request) => {
				let ProviderResponse::ListXattrs { names } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let attrs = names
					.into_iter()
					.flat_map(|name| {
						let mut bytes = name.into_bytes();
						bytes.push(0);
						bytes.into_iter()
					})
					.collect::<Vec<_>>();
				if request.size == 0 {
					let response = fuse_getxattr_out {
						padding: 0,
						size: attrs.len().to_u32().unwrap(),
					};
					Ok(Response::ListXattr(response.as_bytes().to_vec()))
				} else if request.size.to_usize().unwrap() < attrs.len() {
					Err(Error::from_raw_os_error(libc::ERANGE))
				} else {
					Ok(Response::ListXattr(attrs))
				}
			},
			RequestData::Lookup(_) => {
				let ProviderResponse::Lookup { attrs, id } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let node = id.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
				let attrs = attrs.ok_or_else(|| Error::from_raw_os_error(libc::EIO))?;
				let out = Self::fuse_entry_out_from_attrs(node, attrs);
				Ok(Response::Lookup(out))
			},
			RequestData::Open(_) => {
				let ProviderResponse::Open { backing_fd, handle } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let mut open_flags = sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE;
				let mut backing_id = -1;
				if self.passthrough_enabled {
					let Some(backing_fd) = backing_fd else {
						if self.passthrough_required {
							tracing::error!(
								fh = handle,
								"passthrough is required but the provider did not supply a backing fd"
							);
							return Err(Error::from_raw_os_error(libc::EOPNOTSUPP));
						}
						let out = fuse_open_out {
							backing_id,
							fh: handle,
							open_flags,
						};
						return Ok(Response::Open(out));
					};
					match self.register_passthrough_backing(fd, handle, &backing_fd) {
						Err(error) => {
							if error.raw_os_error() == Some(libc::EPERM)
								&& !self
									.passthrough_permission_warning_emitted
									.swap(true, Ordering::Relaxed)
							{
								tracing::warn!(
									"failed to register a passthrough backing due to missing CAP_SYS_ADMIN; falling back to regular fuse I/O"
								);
							}
							if self.passthrough_required {
								tracing::error!(
									?error,
									fh = handle,
									"passthrough is required but backing registration failed"
								);
								return Err(error);
							}
							tracing::trace!(
								?error,
								fh = handle,
								"failed to register passthrough backing"
							);
						},
						Ok(id) => {
							open_flags = sys::FOPEN_PASSTHROUGH;
							backing_id = id;
						},
					}
				}
				let out = fuse_open_out {
					backing_id,
					fh: handle,
					open_flags,
				};
				Ok(Response::Open(out))
			},
			RequestData::OpenDir(_) => {
				let ProviderResponse::OpenDir { handle } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let out = fuse_open_out {
					backing_id: -1,
					fh: handle,
					open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
				};
				Ok(Response::OpenDir(out))
			},
			RequestData::Read(_) => {
				let ProviderResponse::Read { bytes } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				Ok(Response::Read(bytes))
			},
			RequestData::ReadDir(request) => {
				let ProviderResponse::ReadDir { entries } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let response = Self::build_read_dir_response(entries, *request);
				Ok(response)
			},
			RequestData::ReadDirPlus(request) => {
				let ProviderResponse::ReadDirPlus { entries } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let response = self.build_read_dir_plus_response(entries, *request);
				Ok(response)
			},
			RequestData::ReadLink => {
				let ProviderResponse::ReadLink { target } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				Self::read_link_response(target)
			},
			RequestData::Statx(data) => {
				let ProviderResponse::GetAttr { attrs } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let attr = Self::fuse_attr_out(request.header.nodeid, attrs);
				let out = Self::fuse_statx_out(attr, data.getattr_flags);
				Ok(Response::Statx(out))
			},
			_ => Err(Error::other("unexpected provider batch request")),
		}
	}

	#[must_use]
	fn build_read_dir_response(
		entries: Vec<(String, u64, crate::EntryKind)>,
		request: fuse_read_in,
	) -> Response {
		// Encode entries that fit in the requested buffer.
		let entries = entries.into_iter().enumerate();
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node, type_)) in entries {
			let type_ = Self::fuse_dirent_type(type_);
			let name = name.into_bytes();
			let struct_size = std::mem::size_of::<FuseDirentHeader>();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let offset = request.offset.saturating_add(offset.to_u64().unwrap());
			let dirent = FuseDirentHeader {
				ino: crate::readdir_inode(node),
				namelen: name.len().to_u32().unwrap(),
				off: offset.saturating_add(1),
				type_,
			};
			response.extend_from_slice(dirent.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}

		Response::ReadDir(response)
	}

	fn build_read_dir_plus_response(
		&self,
		entries: Vec<(String, u64, crate::Attrs)>,
		request: fuse_read_in,
	) -> Response {
		// Encode entries that fit in the requested buffer.
		let mut entries = entries.into_iter().enumerate();
		let mut nodes = Vec::new();
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		while let Some((offset, (name, node, attr))) = entries.next() {
			let offset = request.offset.to_usize().unwrap() + offset;
			let name = name.into_bytes();
			let struct_size = std::mem::size_of::<FuseDirentPlusHeader>();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				// Roll back the lookup references that do not fit.
				self.provider.forget_sync(node, 1);
				for (_, (_, node, _)) in entries {
					self.provider.forget_sync(node, 1);
				}
				break;
			}

			let type_ = match attr.inner {
				AttrsInner::Directory => S_IFDIR,
				AttrsInner::File { .. } => S_IFREG,
				AttrsInner::Symlink { .. } => S_IFLNK,
			};

			let dirent = FuseDirentHeader {
				ino: node,
				namelen: name.len().to_u32().unwrap(),
				off: offset.to_u64().unwrap() + 1,
				type_,
			};

			let entry = FuseDirentPlusHeader {
				dirent,
				entry_out: Self::fuse_entry_out_from_attrs(node, attr),
			};
			response.extend_from_slice(entry.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
			nodes.push(node);
		}

		Response::ReadDirPlus {
			data: response,
			nodes,
		}
	}

	pub(super) fn read_link_response(target: Bytes) -> Result<Response> {
		if target.contains(&0) {
			return Err(Error::from_raw_os_error(libc::EIO));
		}

		Ok(Response::ReadLink(target))
	}

	fn register_passthrough_backing(
		&self,
		fd: &OwnedFd,
		fh: u64,
		backing_fd: &OwnedFd,
	) -> Result<i32> {
		let mut map = sys::fuse_backing_map {
			fd: backing_fd.as_raw_fd(),
			flags: 0,
			padding: 0,
		};

		// Register the backing descriptor with the kernel.
		// SAFETY: The ioctl receives a valid backing map for the duration of the call, and both
		// descriptors remain open while the kernel consumes it.
		let backing_id = unsafe {
			ioctl::ioctl(
				fd,
				IoctlPointerInt::<FUSE_DEV_IOC_BACKING_OPEN, _>::new(&mut map),
			)
		}
		.map_err(Error::from)?;
		let backing_id_u32 = backing_id
			.to_u32()
			.ok_or_else(|| Error::other("invalid backing id"))?;

		// Track the backing ID for release.
		self.passthrough_backing_ids
			.lock()
			.unwrap()
			.insert(fh, backing_id_u32);
		Ok(backing_id)
	}

	pub(super) fn close_passthrough_backing(&self, fd: &OwnedFd, fh: u64) {
		let backing_id = self.passthrough_backing_ids.lock().unwrap().remove(&fh);
		let Some(backing_id) = backing_id else {
			return;
		};
		let mut backing_id = backing_id;
		// SAFETY: The ioctl receives a valid pointer to the registered backing ID, and the FUSE
		// descriptor remains open for the duration of the call.
		let ret = unsafe {
			ioctl::ioctl(
				fd,
				IoctlPointerInt::<FUSE_DEV_IOC_BACKING_CLOSE, _>::new(&mut backing_id),
			)
		};
		if let Err(error) = ret {
			let error: Error = error.into();
			tracing::error!(?error, %fh, %backing_id, "failed to close passthrough backing");
		}
	}

	#[must_use]
	fn statfs_response() -> Response {
		let out = sys::fuse_statfs_out {
			st: sys::fuse_kstatfs {
				bavail: u64::MAX / 2,
				bfree: u64::MAX / 2,
				blocks: u64::MAX / 2,
				bsize: 65536,
				ffree: u64::MAX / 2,
				files: u64::MAX / 2,
				frsize: 1024,
				namelen: u32::MAX,
				padding: 0,
				spare: [0; 6],
			},
		};
		Response::Statfs(out)
	}

	#[must_use]
	pub(super) fn fuse_statx_out(attr: sys::fuse_attr_out, flags: u32) -> sys::fuse_statx_out {
		// Convert the FUSE timestamps.
		let time = |seconds: u64, nanoseconds: u32| sys::fuse_sx_time {
			__reserved: 0,
			tv_nsec: nanoseconds,
			tv_sec: seconds.to_i64().unwrap_or(i64::MAX),
		};

		// Build the STATX response.
		sys::fuse_statx_out {
			attr_valid: attr.attr_valid,
			attr_valid_nsec: attr.attr_valid_nsec,
			flags,
			spare: [0; 2],
			stat: sys::fuse_statx {
				__spare0: [0],
				__spare2: [0; 14],
				atime: time(attr.attr.atime, attr.attr.atimensec),
				attributes: 0,
				attributes_mask: 0,
				blksize: attr.attr.blksize,
				blocks: attr.attr.blocks,
				btime: time(0, 0),
				ctime: time(attr.attr.ctime, attr.attr.ctimensec),
				dev_major: 0,
				dev_minor: 0,
				gid: attr.attr.gid,
				ino: attr.attr.ino,
				mask: libc::STATX_BASIC_STATS,
				mode: attr.attr.mode.to_u16().unwrap(),
				mtime: time(attr.attr.mtime, attr.attr.mtimensec),
				nlink: attr.attr.nlink,
				rdev_major: 0,
				rdev_minor: 0,
				size: attr.attr.size,
				uid: attr.attr.uid,
			},
		}
	}

	pub(super) fn handle_interrupt_request(&self, request: fuse_interrupt_in) -> Result<Response> {
		if !self.cancel_async_request(request.unique) {
			return Err(Error::from_raw_os_error(libc::EAGAIN));
		}

		Ok(Response::Interrupt)
	}

	fn handle_unsupported_request(header: fuse_in_header, request: u32) -> Result<Response> {
		tracing::trace!(?header, %request, "unsupported request");
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	#[must_use]
	pub(super) fn fuse_attr_out(node: u64, attr: crate::Attrs) -> fuse_attr_out {
		// Derive the inode metadata.
		let (size, mode) = match attr.inner {
			AttrsInner::Directory => (0, S_IFDIR | 0o555),
			AttrsInner::File { executable, size } => (
				size,
				S_IFREG | 0o444 | (if executable { 0o111 } else { 0o000 }),
			),
			AttrsInner::Symlink { size } => (size, S_IFLNK | 0o444),
		};
		let mode = mode.to_u32().unwrap();
		let blocks = size.div_ceil(512);

		// Build the FUSE attributes.
		fuse_attr_out {
			attr: fuse_attr {
				atime: attr.atime.secs,
				atimensec: attr.atime.nanos,
				blksize: 512,
				blocks,
				ctime: attr.ctime.secs,
				ctimensec: attr.ctime.nanos,
				flags: 0,
				gid: attr.gid,
				ino: node,
				mode,
				mtime: attr.mtime.secs,
				mtimensec: attr.mtime.nanos,
				nlink: 1,
				rdev: 0,
				size,
				uid: attr.uid,
			},
			attr_valid: u64::MAX,
			attr_valid_nsec: 0,
			dummy: 0,
		}
	}

	#[must_use]
	fn fuse_entry_out_from_attrs(node: u64, attr: crate::Attrs) -> fuse_entry_out {
		let attr_out = Self::fuse_attr_out(node, attr);
		fuse_entry_out {
			attr: attr_out.attr,
			attr_valid: u64::MAX,
			attr_valid_nsec: 0,
			entry_valid: u64::MAX,
			entry_valid_nsec: 0,
			generation: 0,
			nodeid: node,
		}
	}

	#[must_use]
	fn fuse_dirent_type(kind: crate::EntryKind) -> u32 {
		match kind {
			crate::EntryKind::Directory => S_IFDIR,
			crate::EntryKind::File => S_IFREG,
			crate::EntryKind::Symlink => S_IFLNK,
		}
	}
}
