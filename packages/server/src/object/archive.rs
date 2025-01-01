use crate::Server;
use num::ToPrimitive;
use std::{
	collections::{BTreeSet, VecDeque},
	pin::{pin, Pin},
};
use tangram_client as tg;
use tangram_futures::write::Ext as _;
use tokio::io::{AsyncWrite, AsyncWriteExt as _};

pub const MAGIC_NUMBER: [u8; 4] = *b"tgar";
pub const VERSION: u64 = 0;

impl Server {
	pub(crate) async fn archive_object(
		&self,
		id: &tg::object::Id,
		mut writer: impl AsyncWrite + Unpin + Send + 'static,
	) -> tg::Result<()> {
		// Pin the writer.
		let mut writer = pin!(writer);

		// Write the magic number.
		writer
			.write_all(&MAGIC_NUMBER)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the magic number"))?;

		// Write the version.
		writer
			.write_uvarint(VERSION)
			.await
			.map_err(|source| tg::error!(!source, "failed to write the version"))?;

		// Write the objects.
		let object = tg::Object::with_id(id.clone());
		let mut queue = VecDeque::from([object]);
		let mut visited = BTreeSet::new();
		while let Some(object) = queue.pop_front() {
			// Check if this object has already been written to the archive.
			let id = object.id(self).await?;
			if !visited.insert(id.clone()) {
				continue;
			}

			// Write the ID.
			let bytes = id.to_string().into_bytes();
			let len = bytes.len().to_u64().unwrap();
			write_varint(&mut writer, len).await?;
			writer
				.write_all(&bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the id"))?;

			// Write the object data.
			let bytes = object.data(self).await?.serialize()?;
			let len = bytes.len().to_u64().unwrap();
			write_varint(&mut writer, len).await?;
			writer
				.write_all(&bytes)
				.await
				.map_err(|source| tg::error!(!source, "failed to write the bytes"))?;

			// Add the children to the queue.
			let children = object.children(self).await?;
			queue.extend(children);
		}

		Ok(())
	}
}

async fn write_varint(writer: &mut Pin<&mut impl AsyncWrite>, mut src: u64) -> tg::Result<()> {
	loop {
		let mut byte = (src & 0x7F) as u8;
		src >>= 7;
		if src != 0 {
			byte |= 0x80;
		}
		writer
			.write_all(&[byte])
			.await
			.map_err(|source| tg::error!(!source, "failed to write the varint"))?;
		if src == 0 {
			break;
		}
	}
	Ok(())
}
