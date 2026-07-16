use super::*;

impl<P> Server<P>
where
	P: Provider + Send + Sync + 'static,
{
	pub(super) fn deserialize_uring_request(slot: &UringSlot) -> Result<Request> {
		let in_out = slot.header.in_out.as_bytes();
		let (header, _) = sys::fuse_in_header::read_from_prefix(in_out)
			.map_err(|_| Error::other("failed to deserialize the request header"))?;
		let header_len = size_of::<sys::fuse_in_header>();
		let request_len = header
			.len
			.to_usize()
			.ok_or_else(|| Error::other("failed to deserialize request data"))?;
		if request_len < header_len {
			return Err(Error::other("failed to deserialize request data"));
		}
		let total_extlen = usize::from(header.total_extlen) * 8;
		let Some(request_data_len) = request_len.checked_sub(header_len + total_extlen) else {
			return Err(Error::other("failed to deserialize request data"));
		};
		let payload_size = slot
			.header
			.ring_ent_in_out
			.payload_sz
			.to_usize()
			.ok_or_else(|| Error::other("failed to deserialize request data"))?;
		if payload_size > request_data_len || payload_size > slot.payload.len() {
			return Err(Error::other("failed to deserialize request data"));
		}
		let op_data_len = request_data_len - payload_size;
		let op_in = slot.header.op_in.as_bytes();
		if op_data_len > op_in.len() {
			return Err(Error::other("failed to deserialize request data"));
		}
		let mut bytes = Vec::with_capacity(request_data_len);
		bytes.extend_from_slice(&op_in[..op_data_len]);
		bytes.extend_from_slice(&slot.payload.as_slice()[..payload_size]);
		let data = Self::parse_request_data(&header, &bytes)?;
		let request = Request { header, data };
		Ok(request)
	}

	pub(super) fn deserialize_request(buffer: &[u8]) -> Result<Request> {
		let (header, _) = sys::fuse_in_header::read_from_prefix(buffer)
			.map_err(|_| Error::other("failed to deserialize the request header"))?;
		let header_len = std::mem::size_of::<sys::fuse_in_header>();
		let request_len = header
			.len
			.to_usize()
			.ok_or_else(|| Error::other("failed to deserialize request data"))?;
		if request_len < header_len || request_len > buffer.len() {
			return Err(Error::other("failed to deserialize request data"));
		}
		let total_extlen = usize::from(header.total_extlen) * 8;
		let data = &buffer[header_len..request_len];
		let Some(data_len) = data.len().checked_sub(total_extlen) else {
			return Err(Error::other("failed to deserialize request data"));
		};
		let data = &data[..data_len];
		let data = Self::parse_request_data(&header, data)?;
		let request = Request { header, data };
		Ok(request)
	}

	fn parse_request_data(header: &fuse_in_header, data: &[u8]) -> Result<RequestData> {
		let data = match header.opcode {
			sys::fuse_opcode_FUSE_BATCH_FORGET => {
				if data.len() < size_of::<sys::fuse_batch_forget_in>() {
					return Err(Error::other("failed to deserialize request data"));
				}
				let (batch, entries) = data.split_at(size_of::<sys::fuse_batch_forget_in>());
				let batch: sys::fuse_batch_forget_in = read_data(batch)?;
				let count = batch
					.count
					.to_usize()
					.ok_or_else(|| Error::other("failed to deserialize request data"))?;
				let one_len = size_of::<sys::fuse_forget_one>();
				let len = count
					.checked_mul(one_len)
					.ok_or_else(|| Error::other("failed to deserialize request data"))?;
				if entries.len() < len {
					return Err(Error::other("failed to deserialize request data"));
				}
				let entries = entries[..len]
					.chunks_exact(one_len)
					.map(read_data::<sys::fuse_forget_one>)
					.collect::<Result<Vec<_>>>()?;
				RequestData::BatchForget(batch, entries)
			},
			sys::fuse_opcode_FUSE_DESTROY => RequestData::Destroy,
			sys::fuse_opcode_FUSE_FLUSH => RequestData::Flush(read_data(data)?),
			sys::fuse_opcode_FUSE_FORGET => RequestData::Forget(read_data(data)?),
			sys::fuse_opcode_FUSE_GETATTR => RequestData::GetAttr(read_data(data)?),
			sys::fuse_opcode_FUSE_GETXATTR => {
				if data.len() < std::mem::size_of::<sys::fuse_getxattr_in>() {
					return Err(Error::other("failed to deserialize request data"));
				}
				let (fuse_getxattr_in, name) =
					data.split_at(std::mem::size_of::<sys::fuse_getxattr_in>());
				let fuse_getxattr_in = read_data(fuse_getxattr_in)?;
				let name = CString::from_vec_with_nul(name.to_owned())
					.map_err(|_| Error::other("failed to deserialize request data"))?;
				RequestData::GetXattr(fuse_getxattr_in, name)
			},
			sys::fuse_opcode_FUSE_INIT => RequestData::Init(Self::parse_init_request_data(data)?),
			sys::fuse_opcode_FUSE_LISTXATTR => RequestData::ListXattr(read_data(data)?),
			sys::fuse_opcode_FUSE_LOOKUP => {
				let data = CString::from_vec_with_nul(data.to_owned())
					.map_err(|_| Error::other("failed to deserialize request data"))?;
				RequestData::Lookup(data)
			},
			sys::fuse_opcode_FUSE_OPEN => RequestData::Open(read_data(data)?),
			sys::fuse_opcode_FUSE_OPENDIR => RequestData::OpenDir(read_data(data)?),
			sys::fuse_opcode_FUSE_READ => RequestData::Read(read_data(data)?),
			sys::fuse_opcode_FUSE_READDIR => RequestData::ReadDir(read_data(data)?),
			sys::fuse_opcode_FUSE_READDIRPLUS => RequestData::ReadDirPlus(read_data(data)?),
			sys::fuse_opcode_FUSE_READLINK => RequestData::ReadLink,
			sys::fuse_opcode_FUSE_RELEASE => RequestData::Release(read_data(data)?),
			sys::fuse_opcode_FUSE_RELEASEDIR => RequestData::ReleaseDir(read_data(data)?),
			sys::fuse_opcode_FUSE_STATFS => RequestData::Statfs,
			sys::fuse_opcode_FUSE_STATX => RequestData::Statx(read_data(data)?),
			sys::fuse_opcode_FUSE_INTERRUPT => RequestData::Interrupt(read_data(data)?),
			_ => RequestData::Unsupported(header.opcode),
		};
		Ok(data)
	}

	fn parse_init_request_data(data: &[u8]) -> Result<sys::fuse_init_in> {
		if data.len() >= size_of::<sys::fuse_init_in>() {
			return read_data::<sys::fuse_init_in>(data);
		}
		if data.len() >= size_of::<FuseInitInV7p6>() {
			let data = read_data::<FuseInitInV7p6>(data)?;
			return Ok(sys::fuse_init_in {
				major: data.major,
				minor: data.minor,
				max_readahead: data.max_readahead,
				flags: data.flags,
				flags2: 0,
				unused: [0; 11],
			});
		}
		if data.len() >= size_of::<FuseInitInV7p1>() {
			let data = read_data::<FuseInitInV7p1>(data)?;
			return Ok(sys::fuse_init_in {
				major: data.major,
				minor: data.minor,
				max_readahead: 0,
				flags: 0,
				flags2: 0,
				unused: [0; 11],
			});
		}
		Err(Error::other("failed to deserialize the init request data"))
	}
}

fn read_data<T>(request_data: &[u8]) -> Result<T>
where
	T: zerocopy::FromBytes,
{
	T::read_from_prefix(request_data)
		.map(|(data, _)| data)
		.map_err(|_| Error::other("failed to deserialize the request data"))
}
