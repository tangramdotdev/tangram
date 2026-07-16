use super::*;

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn handle_request_sync_batch(
		&self,
		fd: RawFd,
		requests: &[PendingRequest],
		batch_results: &mut Vec<Result<Option<Response>>>,
	) -> Result<()> {
		enum BatchAction {
			Ready(Result<Option<Response>>),
			NeedsProvider,
		}

		let mut actions = Vec::with_capacity(requests.len());
		let mut provider_requests = Vec::<ProviderRequest>::with_capacity(requests.len());

		for pending_request in requests {
			let request = &pending_request.request;
			match &request.data {
				RequestData::BatchForget(data, entries) => {
					actions.push(BatchAction::Ready(Ok(Some(
						self.handle_batch_forget_request_sync(request.header, *data, entries),
					))));
				},
				RequestData::Destroy => {
					actions.push(BatchAction::Ready(Ok(Some(Response::Destroy))));
				},
				RequestData::Flush(data) => {
					actions.push(BatchAction::Ready(Ok(Some(
						Self::handle_flush_request_sync(request.header, *data),
					))));
				},
				RequestData::Forget(data) => {
					actions.push(BatchAction::Ready(Ok(Some(
						self.handle_forget_request_sync(request.header, *data),
					))));
				},
				RequestData::GetAttr(_) => {
					provider_requests.push(ProviderRequest::GetAttr {
						id: request.header.nodeid,
					});
					actions.push(BatchAction::NeedsProvider);
				},
				RequestData::GetXattr(_, name) => {
					let Ok(name) = name.to_str() else {
						actions.push(BatchAction::Ready(Err(Error::from_raw_os_error(
							libc::ENODATA,
						))));
						continue;
					};
					let name = name.to_owned();
					provider_requests.push(ProviderRequest::GetXattr {
						id: request.header.nodeid,
						name,
					});
					actions.push(BatchAction::NeedsProvider);
				},
				RequestData::ListXattr(_) => {
					provider_requests.push(ProviderRequest::ListXattrs {
						id: request.header.nodeid,
					});
					actions.push(BatchAction::NeedsProvider);
				},
				RequestData::Lookup(name) => {
					let Ok(name) = name.to_str() else {
						actions.push(BatchAction::Ready(Err(Error::from_raw_os_error(
							libc::ENOENT,
						))));
						continue;
					};
					let name = name.to_owned();
					provider_requests.push(ProviderRequest::LookupAndRemember {
						id: request.header.nodeid,
						name,
					});
					actions.push(BatchAction::NeedsProvider);
				},
				RequestData::Open(data) => {
					if let Err(error) = Self::validate_open_request(*data) {
						actions.push(BatchAction::Ready(Err(error)));
					} else {
						provider_requests.push(ProviderRequest::Open {
							id: request.header.nodeid,
						});
						actions.push(BatchAction::NeedsProvider);
					}
				},
				RequestData::OpenDir(data) => {
					if let Err(error) = Self::validate_open_request(*data) {
						actions.push(BatchAction::Ready(Err(error)));
					} else if self.no_opendir_support {
						actions.push(BatchAction::Ready(Err(Error::from_raw_os_error(
							libc::ENOSYS,
						))));
					} else {
						provider_requests.push(ProviderRequest::OpenDir {
							id: request.header.nodeid,
						});
						actions.push(BatchAction::NeedsProvider);
					}
				},
				RequestData::Read(data) => {
					provider_requests.push(ProviderRequest::Read {
						handle: data.fh,
						position: data.offset,
						length: data.size.to_u64().unwrap(),
					});
					actions.push(BatchAction::NeedsProvider);
				},
				RequestData::ReadDir(data) => {
					if self.no_opendir_support {
						let result = self
							.provider
							.readdir_node_sync(
								request.header.nodeid,
								data.offset,
								data.size.to_u64().unwrap(),
							)
							.map(|entries| Self::build_read_dir_response_sync(entries, *data))
							.map(Some);
						actions.push(BatchAction::Ready(result));
					} else {
						provider_requests.push(ProviderRequest::ReadDir {
							handle: data.fh,
							length: data.size.to_u64().unwrap(),
							offset: data.offset,
						});
						actions.push(BatchAction::NeedsProvider);
					}
				},
				RequestData::ReadDirPlus(data) => {
					if self.no_opendir_support {
						let result = self
							.provider
							.readdirplus_node_sync(
								request.header.nodeid,
								data.offset.to_u64().unwrap(),
								data.size.to_u64().unwrap(),
							)
							.map(|entries| self.build_read_dir_plus_response_sync(entries, *data))
							.map(Some);
						actions.push(BatchAction::Ready(result));
					} else {
						provider_requests.push(ProviderRequest::ReadDirPlus {
							handle: data.fh,
							length: data.size.to_u64().unwrap(),
							offset: data.offset.to_u64().unwrap(),
						});
						actions.push(BatchAction::NeedsProvider);
					}
				},
				RequestData::ReadLink => {
					provider_requests.push(ProviderRequest::ReadLink {
						id: request.header.nodeid,
					});
					actions.push(BatchAction::NeedsProvider);
				},
				RequestData::Release(data) => {
					actions.push(BatchAction::Ready(Ok(Some(
						self.handle_release_request_sync(fd, request.header, *data),
					))));
				},
				RequestData::ReleaseDir(data) => {
					actions.push(BatchAction::Ready(Ok(Some(
						self.handle_release_dir_request_sync(fd, request.header, *data),
					))));
				},
				RequestData::Statfs => {
					actions.push(BatchAction::Ready(Ok(Some(
						Self::handle_statfs_request_sync(request.header),
					))));
				},
				RequestData::Statx(data) => {
					actions.push(BatchAction::Ready(Self::sync_result_from_response(
						self.handle_statx_request_sync(request.header, *data),
					)));
				},
				RequestData::Init(_) => {
					actions.push(BatchAction::Ready(Err(Error::other(
						"unexpected init request",
					))));
				},
				RequestData::Interrupt(data) => {
					actions.push(BatchAction::Ready(
						self.handle_interrupt_request_sync(request.header, *data),
					));
				},
				RequestData::Unsupported(opcode) => {
					actions.push(BatchAction::Ready(Self::sync_result_from_response(
						Self::handle_unsupported_request_sync(request.header, *opcode),
					)));
				},
			}
		}

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
					let result = match provider_result {
						Ok(response) => {
							self.map_provider_batch_response(fd, &pending_request.request, response)
						},
						Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => Ok(None),
						Err(error) => Err(error),
					};
					*action = BatchAction::Ready(result);
				}
			}
		}

		batch_results.clear();
		batch_results.reserve(requests.len());
		for (action, request) in std::iter::zip(actions, requests) {
			match action {
				BatchAction::Ready(result) => {
					self.register_response_resources(request.request.header.unique, &result)?;
					batch_results.push(result);
				},
				BatchAction::NeedsProvider => {
					return Err(Error::other("missing provider batch result"));
				},
			}
		}
		Ok(())
	}

	pub(super) fn map_provider_batch_response(
		&self,
		fd: RawFd,
		request: &Request,
		response: ProviderResponse,
	) -> Result<Option<Response>> {
		match &request.data {
			RequestData::GetAttr(_) => {
				let ProviderResponse::GetAttr { attrs } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let out = Self::fuse_attr_out(request.header.nodeid, attrs);
				Ok(Some(Response::GetAttr(out)))
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
						size: attr.len().to_u32().unwrap(),
						padding: 0,
					};
					Ok(Some(Response::GetXattr(response.as_bytes().to_vec())))
				} else if request.size.to_usize().unwrap() < attr.len() {
					Err(Error::from_raw_os_error(libc::ERANGE))
				} else {
					Ok(Some(Response::GetXattr(attr)))
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
						size: attrs.len().to_u32().unwrap(),
						padding: 0,
					};
					Ok(Some(Response::ListXattr(response.as_bytes().to_vec())))
				} else if request.size.to_usize().unwrap() < attrs.len() {
					Err(Error::from_raw_os_error(libc::ERANGE))
				} else {
					Ok(Some(Response::ListXattr(attrs)))
				}
			},
			RequestData::Lookup(_) => {
				let ProviderResponse::Lookup { attrs, id } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let node = id.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
				let attrs = attrs.ok_or_else(|| Error::from_raw_os_error(libc::EIO))?;
				let out = Self::fuse_entry_out_from_attrs(node, attrs);
				Ok(Some(Response::Lookup(out)))
			},
			RequestData::Open(_) => {
				let ProviderResponse::Open { handle, backing_fd } = response else {
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
							self.provider.close_sync(handle);
							return Err(Error::from_raw_os_error(libc::EOPNOTSUPP));
						}
						let out = fuse_open_out {
							fh: handle,
							open_flags,
							backing_id,
						};
						return Ok(Some(Response::Open(out)));
					};
					match self.register_passthrough_backing(fd, handle, &backing_fd) {
						Ok(id) => {
							open_flags = sys::FOPEN_PASSTHROUGH;
							backing_id = id;
						},
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
								self.provider.close_sync(handle);
								return Err(error);
							}
							tracing::trace!(
								?error,
								fh = handle,
								"failed to register passthrough backing"
							);
						},
					}
				}
				let out = fuse_open_out {
					fh: handle,
					open_flags,
					backing_id,
				};
				Ok(Some(Response::Open(out)))
			},
			RequestData::OpenDir(_) => {
				let ProviderResponse::OpenDir { handle } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let out = fuse_open_out {
					fh: handle,
					open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
					backing_id: -1,
				};
				Ok(Some(Response::OpenDir(out)))
			},
			RequestData::Read(_) => {
				let ProviderResponse::Read { bytes } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				Ok(Some(Response::Read(bytes)))
			},
			RequestData::ReadDir(request) => {
				let ProviderResponse::ReadDir { entries } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let response = Self::build_read_dir_response_sync(entries, *request);
				Ok(Some(response))
			},
			RequestData::ReadDirPlus(request) => {
				let ProviderResponse::ReadDirPlus { entries } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				let response = self.build_read_dir_plus_response_sync(entries, *request);
				Ok(Some(response))
			},
			RequestData::ReadLink => {
				let ProviderResponse::ReadLink { target } = response else {
					return Err(Error::from_raw_os_error(libc::EIO));
				};
				Self::read_link_response(target).map(Some)
			},
			_ => Err(Error::other("unexpected provider batch request")),
		}
	}

	fn sync_result_from_response(result: Result<Response>) -> Result<Option<Response>> {
		match result {
			Ok(response) => Ok(Some(response)),
			Err(error) if error.raw_os_error() == Some(libc::ENOSYS) => Ok(None),
			Err(error) => Err(error),
		}
	}

	pub(super) async fn handle_request(&self, request: Request) -> Result<Option<Response>> {
		let unique = request.header.unique;
		let result = match request.data {
			RequestData::BatchForget(data, entries) => {
				self.handle_batch_forget_request(request.header, data, entries)
					.await
			},
			RequestData::Destroy => Ok(Some(Response::Destroy)),
			RequestData::Flush(data) => self.handle_flush_request(request.header, data).await,
			RequestData::Forget(data) => self.handle_forget_request(request.header, data).await,
			RequestData::GetAttr(data) => self.handle_get_attr_request(request.header, data).await,
			RequestData::GetXattr(data, name) => {
				self.handle_get_xattr_request(request.header, data, name)
					.await
			},
			RequestData::Init(_) => Err(Error::other("unexpected init request")),
			RequestData::ListXattr(data) => {
				self.handle_list_xattr_request(request.header, data).await
			},
			RequestData::Lookup(data) => self.handle_lookup_request(request.header, data).await,
			RequestData::Open(data) => self.handle_open_request(request.header, data).await,
			RequestData::OpenDir(data) => self.handle_open_dir_request(request.header, data).await,
			RequestData::Read(data) => self.handle_read_request(request.header, data).await,
			RequestData::ReadDir(data) => {
				self.handle_read_dir_request(request.header, data, false)
					.await
			},
			RequestData::ReadDirPlus(data) => {
				self.handle_read_dir_request(request.header, data, true)
					.await
			},
			RequestData::ReadLink => self.handle_read_link_request(request.header).await,
			RequestData::Release(data) => self.handle_release_request(request.header, data).await,
			RequestData::ReleaseDir(data) => {
				self.handle_release_dir_request(request.header, data).await
			},
			RequestData::Statfs => self.handle_statfs_request(request.header).await,
			RequestData::Statx(data) => self.handle_statx_request(request.header, data).await,
			RequestData::Interrupt(data) => {
				self.handle_interrupt_request(request.header, data).await
			},

			RequestData::Unsupported(opcode) => {
				self.handle_unsupported_request(request.header, opcode)
					.await
			},
		};
		self.register_response_resources(unique, &result)?;

		result
	}
	async fn handle_batch_forget_request(
		&self,
		_header: fuse_in_header,
		request: fuse_batch_forget_in,
		entries: Vec<sys::fuse_forget_one>,
	) -> Result<Option<Response>> {
		let count = request.count.to_usize().unwrap_or(0);
		let requests = entries
			.into_iter()
			.take(count)
			.map(|entry| ProviderRequest::Forget {
				id: entry.nodeid,
				nlookup: entry.nlookup,
			})
			.collect::<Vec<_>>();
		if !requests.is_empty() {
			let _ = self.provider.handle_batch(requests).await;
		}
		Ok(Some(Response::BatchForget))
	}
	pub(super) fn handle_batch_forget_request_sync(
		&self,
		_header: fuse_in_header,
		request: fuse_batch_forget_in,
		entries: &[sys::fuse_forget_one],
	) -> Response {
		let count = request.count.to_usize().unwrap_or(0);
		for entry in entries.iter().take(count) {
			self.provider.forget_sync(entry.nodeid, entry.nlookup);
		}
		Response::BatchForget
	}
	async fn handle_flush_request(
		&self,
		_header: fuse_in_header,
		_request: fuse_flush_in,
	) -> Result<Option<Response>> {
		Ok(Some(Response::Flush))
	}
	fn handle_flush_request_sync(_header: fuse_in_header, _request: fuse_flush_in) -> Response {
		Response::Flush
	}
	async fn handle_forget_request(
		&self,
		header: fuse_in_header,
		request: fuse_forget_in,
	) -> Result<Option<Response>> {
		let _ = self
			.provider
			.handle_batch(vec![ProviderRequest::Forget {
				id: header.nodeid,
				nlookup: request.nlookup,
			}])
			.await;
		Ok(Some(Response::Forget))
	}
	pub(super) fn handle_forget_request_sync(
		&self,
		header: fuse_in_header,
		request: fuse_forget_in,
	) -> Response {
		self.provider.forget_sync(header.nodeid, request.nlookup);
		Response::Forget
	}
	async fn handle_get_attr_request(
		&self,
		header: fuse_in_header,
		_request: fuse_getattr_in,
	) -> Result<Option<Response>> {
		let attr = self.provider.getattr(header.nodeid).await?;
		let out = Self::fuse_attr_out(header.nodeid, attr);
		Ok(Some(Response::GetAttr(out)))
	}
	fn handle_get_attr_request_sync(
		&self,
		header: fuse_in_header,
		_request: fuse_getattr_in,
	) -> Result<Option<Response>> {
		let attr = self.provider.getattr_sync(header.nodeid)?;
		let out = Self::fuse_attr_out(header.nodeid, attr);
		Ok(Some(Response::GetAttr(out)))
	}
	async fn handle_get_xattr_request(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
		name: CString,
	) -> Result<Option<Response>> {
		let name = name
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENODATA))?;
		let attr = self
			.provider
			.getxattr(header.nodeid, name)
			.await?
			.map(|value| value.to_vec())
			.ok_or_else(|| Error::from_raw_os_error(libc::ENODATA))?;

		// If the request size is 0, the driver is requesting the size of the xattr.
		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attr.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::GetXattr(response)))
		} else if request.size.to_usize().unwrap() < attr.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::GetXattr(attr)))
		}
	}
	async fn handle_list_xattr_request(
		&self,
		header: fuse_in_header,
		request: fuse_getxattr_in,
	) -> Result<Option<Response>> {
		let attrs = self
			.provider
			.listxattrs(header.nodeid)
			.await?
			.into_iter()
			.flat_map(|s| {
				let mut s = s.into_bytes();
				s.push(0);
				s.into_iter()
			})
			.collect::<Vec<_>>();

		if request.size == 0 {
			let response = fuse_getxattr_out {
				size: attrs.len().to_u32().unwrap(),
				padding: 0,
			};
			let response = response.as_bytes().to_vec();
			Ok(Some(Response::ListXattr(response)))
		} else if request.size.to_usize().unwrap() < attrs.len() {
			Err(Error::from_raw_os_error(libc::ERANGE))
		} else {
			Ok(Some(Response::ListXattr(attrs)))
		}
	}
	async fn handle_lookup_request(
		&self,
		header: fuse_in_header,
		request: CString,
	) -> Result<Option<Response>> {
		let name = request
			.to_str()
			.map_err(|_| Error::from_raw_os_error(libc::ENOENT))?;
		let (node, attrs) = self
			.provider
			.lookup_and_remember(header.nodeid, name)
			.await?
			.ok_or_else(|| Error::from_raw_os_error(libc::ENOENT))?;
		let out = Self::fuse_entry_out_from_attrs(node, attrs);
		Ok(Some(Response::Lookup(out)))
	}
	async fn handle_open_request(
		&self,
		header: fuse_in_header,
		request: fuse_open_in,
	) -> Result<Option<Response>> {
		Self::validate_open_request(request)?;
		let fh = self.provider.open(header.nodeid).await?;
		if self.passthrough_required {
			self.provider.close(fh).await;
			return Err(Error::from_raw_os_error(libc::EOPNOTSUPP));
		}
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_NOFLUSH | sys::FOPEN_KEEP_CACHE,
			backing_id: -1,
		};
		Ok(Some(Response::Open(out)))
	}
	async fn handle_open_dir_request(
		&self,
		header: fuse_in_header,
		request: fuse_open_in,
	) -> Result<Option<Response>> {
		Self::validate_open_request(request)?;
		if self.no_opendir_support {
			return Err(Error::from_raw_os_error(libc::ENOSYS));
		}
		let fh = self.provider.opendir(header.nodeid).await?;
		let out = fuse_open_out {
			fh,
			open_flags: sys::FOPEN_CACHE_DIR | sys::FOPEN_KEEP_CACHE,
			backing_id: -1,
		};
		Ok(Some(Response::OpenDir(out)))
	}

	pub(super) fn validate_open_request(request: fuse_open_in) -> Result<()> {
		let access_mode = request.flags & libc::O_ACCMODE.to_u32().unwrap();
		if access_mode != libc::O_RDONLY.to_u32().unwrap() {
			return Err(Error::from_raw_os_error(libc::EROFS));
		}

		Ok(())
	}

	async fn handle_read_request(
		&self,
		_header: fuse_in_header,
		request: fuse_read_in,
	) -> Result<Option<Response>> {
		let bytes = self
			.provider
			.read(request.fh, request.offset, request.size.to_u64().unwrap())
			.await?;
		Ok(Some(Response::Read(bytes)))
	}
	async fn handle_read_dir_request(
		&self,
		header: fuse_in_header,
		request: fuse_read_in,
		plus: bool,
	) -> Result<Option<Response>> {
		if plus {
			let offset = request.offset.to_u64().unwrap();
			let length = request.size.to_u64().unwrap();
			let entries = if self.no_opendir_support {
				self.provider
					.readdirplus_node(header.nodeid, offset, length)
					.await?
			} else {
				self.provider
					.readdirplus(request.fh, offset, length)
					.await?
			};
			return Ok(Some(
				self.build_read_dir_plus_response(entries, request).await?,
			));
		}

		let entries = if self.no_opendir_support {
			self.provider
				.readdir_node(
					header.nodeid,
					request.offset,
					request.size.to_u64().unwrap(),
				)
				.await?
		} else {
			self.provider
				.readdir(request.fh, request.offset, request.size.to_u64().unwrap())
				.await?
		};

		let struct_size = std::mem::size_of::<FuseDirentHeader>();

		let entries = entries.into_iter().enumerate();
		let mut response = Vec::with_capacity(request.size.to_usize().unwrap());
		for (offset, (name, node, inner)) in entries {
			let type_ = Self::fuse_dirent_type(inner);
			let name = name.into_bytes();
			let padding = (8 - (struct_size + name.len()) % 8) % 8;
			let entry_size = struct_size + name.len() + padding;
			if response.len() + entry_size > request.size.to_usize().unwrap() {
				break;
			}

			let offset = request.offset.saturating_add(offset.to_u64().unwrap());
			let dirent = FuseDirentHeader {
				ino: node,
				off: offset.saturating_add(1),
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			response.extend_from_slice(dirent.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}

		Ok(Some(Response::ReadDir(response)))
	}
	fn build_read_dir_response_sync(
		entries: Vec<(String, u64, crate::EntryKind)>,
		request: fuse_read_in,
	) -> Response {
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
				ino: node,
				off: offset.saturating_add(1),
				namelen: name.len().to_u32().unwrap(),
				type_,
			};
			response.extend_from_slice(dirent.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
		}
		Response::ReadDir(response)
	}

	async fn build_read_dir_plus_response(
		&self,
		entries: Vec<(String, u64, crate::Attrs)>,
		request: fuse_read_in,
	) -> Result<Response> {
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
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			let entry = FuseDirentPlusHeader {
				entry_out: Self::fuse_entry_out_from_attrs(node, attr),
				dirent,
			};
			response.extend_from_slice(entry.as_bytes());
			response.extend_from_slice(&name);
			response.extend((0..padding).map(|_| 0));
			nodes.push(node);
		}

		Ok(Response::ReadDirPlus {
			data: response,
			nodes,
		})
	}

	fn build_read_dir_plus_response_sync(
		&self,
		entries: Vec<(String, u64, crate::Attrs)>,
		request: fuse_read_in,
	) -> Response {
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
				off: offset.to_u64().unwrap() + 1,
				namelen: name.len().to_u32().unwrap(),
				type_,
			};

			let entry = FuseDirentPlusHeader {
				entry_out: Self::fuse_entry_out_from_attrs(node, attr),
				dirent,
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
	async fn handle_read_link_request(&self, header: fuse_in_header) -> Result<Option<Response>> {
		let target = self.provider.readlink(header.nodeid).await?;
		Self::read_link_response(target).map(Some)
	}

	pub(super) fn read_link_response(target: Bytes) -> Result<Response> {
		if target.contains(&0) {
			return Err(Error::from_raw_os_error(libc::EIO));
		}

		Ok(Response::ReadLink(target))
	}
	async fn handle_release_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
		self.provider.close(request.fh).await;
		Ok(Some(Response::Release))
	}
	fn handle_release_request_sync(
		&self,
		fd: RawFd,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Response {
		self.close_passthrough_backing(fd, request.fh);
		self.provider.close_sync(request.fh);
		Response::Release
	}
	async fn handle_release_dir_request(
		&self,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Result<Option<Response>> {
		if self.no_opendir_support {
			return Ok(Some(Response::ReleaseDir));
		}
		self.provider.close(request.fh).await;
		Ok(Some(Response::ReleaseDir))
	}
	fn handle_release_dir_request_sync(
		&self,
		fd: RawFd,
		_header: fuse_in_header,
		request: fuse_release_in,
	) -> Response {
		if self.no_opendir_support {
			return Response::ReleaseDir;
		}
		self.close_passthrough_backing(fd, request.fh);
		self.provider.close_sync(request.fh);
		Response::ReleaseDir
	}
	fn register_passthrough_backing(
		&self,
		fd: RawFd,
		fh: u64,
		backing_fd: &OwnedFd,
	) -> Result<i32> {
		let mut map = sys::fuse_backing_map {
			fd: backing_fd.as_raw_fd(),
			flags: 0,
			padding: 0,
		};
		let fd = unsafe { BorrowedFd::borrow_raw(fd) };
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
		self.passthrough_backing_ids
			.lock()
			.unwrap()
			.insert(fh, backing_id_u32);
		Ok(backing_id)
	}

	pub(super) fn close_passthrough_backing(&self, fd: RawFd, fh: u64) {
		let backing_id = self.passthrough_backing_ids.lock().unwrap().remove(&fh);
		let Some(backing_id) = backing_id else {
			return;
		};
		let mut backing_id = backing_id;
		let fd = unsafe { BorrowedFd::borrow_raw(fd) };
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
	async fn handle_statfs_request(
		&self,
		_header: sys::fuse_in_header,
	) -> Result<Option<Response>> {
		let out = sys::fuse_statfs_out {
			st: sys::fuse_kstatfs {
				blocks: u64::MAX / 2,
				bfree: u64::MAX / 2,
				bavail: u64::MAX / 2,
				files: u64::MAX / 2,
				ffree: u64::MAX / 2,
				bsize: 65536,
				namelen: u32::MAX,
				frsize: 1024,
				padding: 0,
				spare: [0; 6],
			},
		};
		Ok(Some(Response::Statfs(out)))
	}
	fn handle_statfs_request_sync(_header: sys::fuse_in_header) -> Response {
		let out = sys::fuse_statfs_out {
			st: sys::fuse_kstatfs {
				blocks: u64::MAX / 2,
				bfree: u64::MAX / 2,
				bavail: u64::MAX / 2,
				files: u64::MAX / 2,
				ffree: u64::MAX / 2,
				bsize: 65536,
				namelen: u32::MAX,
				frsize: 1024,
				padding: 0,
				spare: [0; 6],
			},
		};
		Response::Statfs(out)
	}
	async fn handle_statx_request(
		&self,
		header: sys::fuse_in_header,
		request: sys::fuse_statx_in,
	) -> Result<Option<Response>> {
		let Some(Response::GetAttr(attr)) = self
			.handle_get_attr_request(header, {
				sys::fuse_getattr_in {
					getattr_flags: request.getattr_flags,
					dummy: 0,
					fh: request.fh,
				}
			})
			.await?
		else {
			return Ok(None);
		};
		let out = Self::fuse_statx_out(attr, request.getattr_flags);
		Ok(Some(Response::Statx(out)))
	}
	fn handle_statx_request_sync(
		&self,
		header: sys::fuse_in_header,
		request: sys::fuse_statx_in,
	) -> Result<Response> {
		let Some(Response::GetAttr(attr)) = self.handle_get_attr_request_sync(header, {
			sys::fuse_getattr_in {
				getattr_flags: request.getattr_flags,
				dummy: 0,
				fh: request.fh,
			}
		})?
		else {
			return Err(Error::other("failed to get the attr response"));
		};
		let out = Self::fuse_statx_out(attr, request.getattr_flags);
		Ok(Response::Statx(out))
	}

	pub(super) fn fuse_statx_out(attr: sys::fuse_attr_out, flags: u32) -> sys::fuse_statx_out {
		let time = |seconds: u64, nanoseconds: u32| sys::fuse_sx_time {
			__reserved: 0,
			tv_nsec: nanoseconds,
			tv_sec: seconds.to_i64().unwrap_or(i64::MAX),
		};
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
				blocks: attr.attr.blocks,
				blksize: attr.attr.blksize,
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
	async fn handle_interrupt_request(
		&self,
		_header: fuse_in_header,
		request: fuse_interrupt_in,
	) -> Result<Option<Response>> {
		if !self.cancel_async_request(request.unique) {
			return Err(Error::from_raw_os_error(libc::EAGAIN));
		}

		Ok(Some(Response::Interrupt))
	}
	pub(super) fn handle_interrupt_request_sync(
		&self,
		_header: fuse_in_header,
		request: fuse_interrupt_in,
	) -> Result<Option<Response>> {
		if !self.cancel_async_request(request.unique) {
			return Err(Error::from_raw_os_error(libc::EAGAIN));
		}

		Ok(Some(Response::Interrupt))
	}
	async fn handle_unsupported_request(
		&self,
		header: fuse_in_header,
		request: u32,
	) -> Result<Option<Response>> {
		tracing::trace!(?header, %request, "unsupported request");
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}
	fn handle_unsupported_request_sync(header: fuse_in_header, request: u32) -> Result<Response> {
		tracing::trace!(?header, %request, "unsupported request");
		Err(Error::from_raw_os_error(libc::ENOSYS))
	}

	pub(super) fn fuse_attr_out(node: u64, attr: crate::Attrs) -> fuse_attr_out {
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
		fuse_attr_out {
			attr_valid: u64::MAX,
			attr_valid_nsec: 0,
			attr: fuse_attr {
				ino: node,
				size,
				blocks,
				atime: attr.atime.secs,
				atimensec: attr.atime.nanos,
				mtime: attr.mtime.secs,
				mtimensec: attr.mtime.nanos,
				ctime: attr.ctime.secs,
				ctimensec: attr.ctime.nanos,
				mode,
				nlink: 1,
				uid: attr.uid,
				gid: attr.gid,
				rdev: 0,
				blksize: 512,
				flags: 0,
			},
			dummy: 0,
		}
	}

	fn fuse_entry_out_from_attrs(node: u64, attr: crate::Attrs) -> fuse_entry_out {
		let attr_out = Self::fuse_attr_out(node, attr);
		fuse_entry_out {
			nodeid: node,
			generation: 0,
			entry_valid: u64::MAX,
			attr_valid: u64::MAX,
			entry_valid_nsec: 0,
			attr_valid_nsec: 0,
			attr: attr_out.attr,
		}
	}

	fn fuse_dirent_type(kind: crate::EntryKind) -> u32 {
		match kind {
			crate::EntryKind::Directory => S_IFDIR,
			crate::EntryKind::File => S_IFREG,
			crate::EntryKind::Symlink => S_IFLNK,
		}
	}
}
