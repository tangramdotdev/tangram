use crate::Server;
use bytes::Bytes;
use futures::{stream, Stream};
use num::ToPrimitive;
use tangram_client as tg;
use tangram_futures::read::Ext as _;
use tokio::io::{AsyncRead, AsyncReadExt as _};

impl Server {
	pub(crate) async fn extract_object(
		&self,
		mut reader: impl AsyncRead + Unpin + Send + 'static,
	) -> tg::Result<impl Stream<Item = tg::Result<(tg::object::Id, Bytes)>> + Send + 'static> {
		// Read the magic number.
		let mut magic = [0u8; 4];
		reader
			.read_exact(&mut magic)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the magic number"))?;
		if magic != super::archive::MAGIC_NUMBER {
			return Err(tg::error!("invalid magic number"));
		}

		// Read the version.
		let version = reader
			.read_uvarint()
			.await
			.map_err(|source| tg::error!(!source, "failed to read the version"))?;
		if version > super::archive::VERSION {
			return Err(tg::error!("invalid version"));
		}

		let stream = stream::try_unfold(reader, move |mut reader| async move {
			// Read the ID.
			let Some(len) = reader
				.try_read_uvarint()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the id length"))?
			else {
				return Ok(None);
			};
			let mut buffer = vec![0u8; len.to_usize().unwrap()];
			reader
				.read_exact(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the id"))?;
			let id = String::from_utf8(buffer)
				.map_err(|_| tg::error!("expected a string"))?
				.parse::<tg::object::Id>()?;

			// Read the bytes.
			let len = reader
				.read_uvarint()
				.await
				.map_err(|source| tg::error!(!source, "failed to read the bytes length"))?;
			let mut buffer = vec![0u8; len.to_usize().unwrap()];
			reader
				.read_exact(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read the bytes"))?;
			let bytes = buffer.into();

			// Validate the ID.
			if tg::object::Id::new(id.kind(), &bytes) != id {
				return Err(
					tg::error!(%object = id, "the archive contained an entry with an invalid id"),
				)?;
			}

			Ok(Some(((id, bytes), reader)))
		});

		Ok(stream)
	}
}
