use crate::Server;
use bytes::Bytes;
use indoc::formatdoc;
use num::ToPrimitive;
use tangram_database::{self as db, Connection, Database, Query, Transaction};
use std::collections::{BTreeSet, VecDeque};
use tangram_client as tg;
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

const TGAR_MAGIC: [u8; 4] = *b"tgar";
const TGAR_VERSION: u32 = 1;

impl Server {
	pub(crate) async fn export_archive<W>(
		&self,
		object: tg::Object,
		mut writer: W,
	) -> tg::Result<()>
	where
		W: AsyncWrite,
	{
		// Pin the writer.
		let mut writer = std::pin::pin!(writer);

		// Write the magic number.
		writer
			.write_all(&TGAR_MAGIC)
			.await
			.map_err(|source| tg::error!(!source, "failed to write archive"))?;

		// Write the version.
		writer
			.write_all(&TGAR_VERSION.to_le_bytes())
			.await
			.map_err(|source| tg::error!(!source, "failed to write archive"))?;

		// Begin writing objects.
		let mut queue: VecDeque<_> = vec![object].into();
		let mut visited = BTreeSet::new();
		while let Some(object) = queue.pop_front() {
			// Check if this object has already been written to the archive.
			let id = object.id(self).await?;
			if !visited.insert(id.clone()) {
				continue;
			}

			// Get the object data.
			let data = object.data(self).await?.serialize()?;

			// Write the object.
			write_bytes(&mut writer, id.to_string().into()).await?;
			write_bytes(&mut writer, data).await?;

			// Recurse.
			let children = object.children(self).await?;
			queue.extend(children);
		}

		Ok(())
	}

	pub(crate) async fn import_archive<R>(&self, mut reader: R, progress: Option<crate::progress::Handle<tg::object::import::Output>>) -> tg::Result<tg::object::Id>
	where
		R: AsyncRead,
	{
		// Pin the reader.
		let mut reader = std::pin::pin!(reader);

		// Read the magic number.
		let mut magic = [0u8; 4];
		reader
			.read_exact(&mut magic)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the archive"))?;
		if magic != TGAR_MAGIC {
			return Err(tg::error!("unknown format"));
		}

		// Read the version.
		let mut version = [0u8; 4];
		reader
			.read_exact(&mut version)
			.await
			.map_err(|source| tg::error!(!source, "failed to read the archive"))?;
		if u32::from_le_bytes(version) != TGAR_VERSION {
			return Err(tg::error!("unknown version"));
		}

		// Get a database connection.
		let mut connection = self.database.write_connection().await.map_err(|source| tg::error!(!source, "failed to acquire a database connection"))?;
		let transaction = connection.transaction().await.map_err(|source| tg::error!(!source, "failed to acquire a transaction"))?;

		// Read the objects.
		let mut root_object = None;
		while let Some(id) = try_read_bytes(&mut reader).await? {
			// Read the ID.
			let id = String::from_utf8(id.into())
				.map_err(|_| tg::error!("expected a string"))?
				.parse::<tg::object::Id>()?;

			// Read the data.
			let data = try_read_bytes(&mut reader)
				.await?
				.ok_or_else(|| tg::error!("unexpected eof"))?;

			// Validate.
			if tg::object::Id::new(id.kind(), &data) != id {
				return Err(tg::error!(%object = id, "invalid archive"))?;
			}

			// Store the object.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id)
					values ({p}1)
					on conflict (id) do nothing;
				"
			);
			let params = db::params![id];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Update the root object if necessary.
			if root_object.is_none() {
				root_object.replace(id);
			}
		}
		if let Some(progress) = &progress {
			progress.log(tg::progress::Level::Info, "committing transaction".into());
		}
		transaction.commit().await.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		// Return the root object.
		root_object.ok_or_else(|| tg::error!("empty archive"))
	}
}

async fn write_varint(
	writer: &mut std::pin::Pin<&mut impl AsyncWrite>,
	mut src: u64,
) -> tg::Result<()> {
	loop {
		let mut byte = (src & 0x7F) as u8;
		src >>= 7;
		if src != 0 {
			byte |= 0x80;
		}
		writer
			.write_all(&[byte])
			.await
			.map_err(|source| tg::error!(!source, "failed to write the value"))?;
		if src == 0 {
			break;
		}
	}
	Ok(())
}

async fn write_bytes(
	writer: &mut std::pin::Pin<&mut impl AsyncWrite>,
	src: Bytes,
) -> tg::Result<()> {
	let len = src.len().to_u64().unwrap();
	write_varint(writer, len).await?;
	writer
		.write_all(&src)
		.await
		.map_err(|source| tg::error!(!source, "failed to write value"))?;
	Ok(())
}

async fn try_read_varint(
	reader: &mut std::pin::Pin<&mut impl AsyncRead>,
) -> tg::Result<Option<u64>> {
	let mut result: u64 = 0;
	let mut shift = 0;
	loop {
		let mut byte: u8 = 0;
		let read =  match reader.read_exact(std::slice::from_mut(&mut byte)).await {
			Ok(read) => read,
			Err(ref error) if error.kind() == std::io::ErrorKind::UnexpectedEof => 0,
			Err(source) => {
				eprintln!("{source:#?}");
				return Err(tg::error!(!source, "failed to read the value"));
			}
		};
		if read == 0 && shift == 0 {
			return Ok(None);
		}
		result |= u64::from(byte & 0x7F) << shift;
		if byte & 0x80 == 0 {
			break;
		}
		shift += 7;
	}
	Ok(Some(result))
}

async fn try_read_bytes(
	reader: &mut std::pin::Pin<&mut impl AsyncRead>,
) -> tg::Result<Option<Bytes>> {
	let Some(len) = try_read_varint(reader).await? else {
		return Ok(None);
	};
	let mut buf = vec![0u8; len.to_usize().unwrap()];
	reader
		.read_exact(&mut buf)
		.await
		.map_err(|source| tg::error!(!source, "failed to read the archive"))?;
	Ok(Some(buf.into()))
}

#[cfg(test)]
mod tests {
	use crate::{util::fs::cleanup, Config, Server};
	use futures::FutureExt as _;
	use insta::assert_snapshot;
	use std::panic::AssertUnwindSafe;
	use tangram_client as tg;
	use tangram_temp::Temp;

	#[tokio::test]
	async fn archive() -> tg::Result<()> {
		let temp = Temp::new();
		let config = Config::with_path(temp.path().to_owned());
		let server = Server::start(config).await?;
		let result = AssertUnwindSafe(async {
			let object: tg::Object = tg::directory! {
				"hello.txt" => "hello, world!",
			}
			.into();

			// Stringify the input.
			object.load_recursive(&server).await?;
			let value = tg::Value::Object(object.clone());
			let options = tg::value::print::Options {
				recursive: true,
				style: tg::value::print::Style::Pretty { indentation: "\t" },
			};
			let input = value.print(options);

			// Create the archive.
			let mut archive: Vec<u8> = Vec::new();
			server.export_archive(object.clone(), &mut archive).await?;

			// Import the archive.
			let id = server.import_archive(archive.as_slice(), None).await?;
			let object = tg::Object::with_id(id);

			// Stringify the output.
			object.load_recursive(&server).await?;
			let value = tg::Value::Object(object.clone());
			let options = tg::value::print::Options {
				recursive: true,
				style: tg::value::print::Style::Pretty { indentation: "\t" },
			};
			let output = value.print(options);

			// Validate
			assert_eq!(input, output);
			assert_snapshot!(output, r#""#);

			Ok::<_, tg::Error>(())
		})
		.catch_unwind()
		.await;

		cleanup(temp, server).await;
		result.unwrap()
	}
}
