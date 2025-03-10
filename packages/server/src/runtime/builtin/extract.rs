use super::Runtime;
use crate::{Server, blob::create::Destination, temp::Temp};
use futures::{AsyncReadExt as _, StreamExt as _};
use indoc::indoc;
use num::ToPrimitive as _;
use std::{
	collections::BTreeMap,
	path::{Path, PathBuf},
	pin::Pin,
	sync::Arc,
	time::Duration,
};
use tangram_client::{self as tg};
use tangram_database::{Connection as _, Database as _, Transaction as _};
use tangram_either::Either;
use tangram_futures::read::shared_position_reader::SharedPositionReader;
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt as _, AsyncSeek};
use tokio_util::compat::{FuturesAsyncReadCompatExt as _, TokioAsyncReadCompatExt as _};

#[derive(Debug, Clone)]
enum Artifact {
	Directory {
		id: Option<tg::directory::Id>,
		data: Option<tg::directory::Data>,
		entries: BTreeMap<String, Artifact>,
		depth: Option<u64>,
		count: Option<u64>,
		size: Option<u64>,
		weight: Option<u64>,
	},
	File {
		id: tg::file::Id,
		data: tg::file::Data,
		contents: crate::blob::create::Blob,
		count: u64,
		depth: Option<u64>,
		size: u64,
		weight: u64,
	},
	Symlink {
		id: tg::symlink::Id,
		data: tg::symlink::Data,
		count: u64,
		depth: Option<u64>,
		size: u64,
		weight: u64,
	},
}

impl Runtime {
	pub async fn extract(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the blob.
		let blob: tg::Blob = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_into()
			.ok()
			.ok_or_else(|| tg::error!("expected a blob"))?;

		// Detect archive format.
		let (format, compression) = detect_archive_format(server, &blob).await?;

		// Create the reader.
		let reader = crate::blob::Reader::new(&self.server, blob.clone()).await?;
		let reader = SharedPositionReader::with_reader_and_position(reader, 0)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the shared position reader"))?;

		// Create the log task.
		let position = reader.shared_position();
		let size = blob.length(server).await?;
		let log_task = tokio::spawn({
			let server = server.clone();
			let process = process.clone();
			async move {
				loop {
					let position = position.load(std::sync::atomic::Ordering::Relaxed);
					let indicator = tg::progress::Indicator {
						current: Some(position),
						format: tg::progress::IndicatorFormat::Bytes,
						name: String::new(),
						title: "extracting".to_owned(),
						total: Some(size),
					};
					let message = format!("{indicator}\n");
					let arg = tg::process::log::post::Arg {
						bytes: message.into(),
						remote: process.remote().cloned(),
					};
					if !server
						.try_post_process_log(process.id(), arg)
						.await
						.map_or(true, |ok| ok.added)
					{
						break;
					}
					tokio::time::sleep(Duration::from_secs(1)).await;
				}
			}
		});
		let log_task_abort_handle = log_task.abort_handle();
		scopeguard::defer! {
			log_task_abort_handle.abort();
		};

		// Extract the artifact.
		let artifact = match format {
			tg::artifact::archive::Format::Tar => {
				// If there is a compression format, wrap the reader.
				let reader: Pin<Box<dyn AsyncRead + Send + 'static>> = match compression {
					Some(tg::blob::compress::Format::Bz2) => {
						Box::pin(async_compression::tokio::bufread::BzDecoder::new(reader))
					},
					Some(tg::blob::compress::Format::Gz) => {
						Box::pin(async_compression::tokio::bufread::GzipDecoder::new(reader))
					},
					Some(tg::blob::compress::Format::Xz) => {
						Box::pin(async_compression::tokio::bufread::XzDecoder::new(reader))
					},
					Some(tg::blob::compress::Format::Zstd) => {
						Box::pin(async_compression::tokio::bufread::ZstdDecoder::new(reader))
					},
					None => Box::pin(reader),
				};
				tar(server, reader).await?
			},
			tg::artifact::archive::Format::Zip => zip(server, reader).await?,
		};

		// Abort and await the log task.
		log_task.abort();
		match log_task.await {
			Ok(()) => Ok(()),
			Err(error) if error.is_cancelled() => Ok(()),
			Err(error) => Err(error),
		}
		.unwrap();

		// Log that the extraction finished.
		let message = "finished extracting\n";
		let arg = tg::process::log::post::Arg {
			bytes: message.into(),
			remote: process.remote().cloned(),
		};
		server.try_post_process_log(process.id(), arg).await.ok();

		Ok(artifact.into())
	}
}

async fn tar<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: AsyncRead + Unpin + Send + 'static,
{
	// Create the reader.
	let mut reader = tokio_tar::Archive::new(reader);

	// Create the root.
	let mut root = Artifact::empty_directory().with_depth(0);
	let root_path = PathBuf::from(".");

	// Add the entries.
	let mut iter = reader
		.entries()
		.map_err(|source| tg::error!(!source, "failed to get the entries from the archive"))?;
	while let Some(entry) = iter.next().await {
		let entry = entry
			.map_err(|source| tg::error!(!source, "failed to get the entry from the archive"))?;
		let header = entry.header();
		let path = PathBuf::from(
			entry
				.path()
				.map_err(|source| tg::error!(!source, "failed to get the entry path"))?
				.as_ref(),
		);
		match &header.entry_type() {
			tokio_tar::EntryType::Directory => {
				if path == root_path {
					continue;
				}
				root.add_entry(path, Artifact::empty_directory())?;
			},
			tokio_tar::EntryType::Symlink => {
				let target = header
					.link_name()
					.map_err(|source| tg::error!(!source, "failed to read the symlink target"))?
					.ok_or_else(|| tg::error!("no symlink target stored in the archive"))?;
				let symlink = Artifact::symlink(target)?;
				root.add_entry(path, symlink)?;
			},
			tokio_tar::EntryType::Link => {
				let target = header
					.link_name()
					.map_err(|source| {
						tg::error!(!source, "failed to read the hard link target path")
					})?
					.ok_or_else(|| tg::error!("no hard link target path stored in the archive"))?;
				let target = Path::new(target.as_ref());
				if let Some(artifact) = root
					.find(target)
					.map_err(|source| tg::error!(!source, "could not search artifact"))?
				{
					root.add_entry(path, artifact.clone())?;
				} else {
					return Err(tg::error!(
						"could not find the hard link target in the archive"
					));
				}
			},
			tokio_tar::EntryType::XGlobalHeader
			| tokio_tar::EntryType::XHeader
			| tokio_tar::EntryType::GNULongName
			| tokio_tar::EntryType::GNULongLink => (),
			_ => {
				let mode = header
					.mode()
					.map_err(|source| tg::error!(!source, "failed to read the entry mode"))?;
				let executable = mode & 0o111 != 0;
				let file = Artifact::file(server, entry, executable).await?;
				root.add_entry(path, file)?;
			},
		}
	}

	// Insert and store the result.
	root.persist(server)
		.await
		.map_err(|source| tg::error!(!source, "failed to persist archive contents"))?;

	// Return the result.
	let artifact = root.as_artifact()?;
	Ok(artifact)
}

async fn zip<R>(server: &Server, reader: R) -> tg::Result<tg::Artifact>
where
	R: AsyncBufRead + AsyncSeek + Unpin + Send + 'static,
{
	// Create the reader.
	let mut reader = async_zip::base::read::seek::ZipFileReader::new(reader.compat())
		.await
		.map_err(|source| tg::error!(!source, "failed to create the zip reader"))?;

	// Create the root.
	let mut root = Artifact::empty_directory().with_depth(0);

	for index in 0..reader.file().entries().len() {
		// Get the reader.
		let mut reader = reader
			.reader_with_entry(index)
			.await
			.map_err(|source| tg::error!(!source, "unable to get the entry"))?;

		// Get the path.
		let path = PathBuf::from(
			reader
				.entry()
				.filename()
				.as_str()
				.map_err(|source| tg::error!(!source, "failed to get the entry filename"))?,
		);

		// Check if the entry is a directory.
		let is_dir = reader
			.entry()
			.dir()
			.map_err(|source| tg::error!(!source, "failed to get type of entry"))?;

		// Check if the entry is a symlink.
		let is_symlink = reader
			.entry()
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o120_000 == 0o120_000);

		// Check if the entry is executable.
		let is_executable = reader
			.entry()
			.unix_permissions()
			.is_some_and(|permissions| permissions & 0o000_111 != 0);

		// Create the artifacts.
		if is_dir {
			root.add_entry(path, Artifact::empty_directory())?;
		} else if is_symlink {
			let mut buffer = Vec::new();
			reader
				.read_to_end(&mut buffer)
				.await
				.map_err(|source| tg::error!(!source, "failed to read symlink target"))?;
			let target = core::str::from_utf8(&buffer)
				.map_err(|source| tg::error!(!source, "symlink target not valid UTF-8"))?;
			let symlink = Artifact::symlink(target)?;
			root.add_entry(path, symlink)?;
		} else {
			let file = Artifact::file(server, reader.compat(), is_executable).await?;
			root.add_entry(path, file)?;
		}
	}

	// Insert and store the result.
	root.persist(server)
		.await
		.map_err(|source| tg::error!(!source, "failed to persist archive contents"))?;

	// Return the result.
	let artifact = root.as_artifact()?;
	Ok(artifact)
}

async fn detect_archive_format(
	server: &Server,
	blob: &tg::Blob,
) -> tg::Result<(
	tg::artifact::archive::Format,
	Option<tg::blob::compress::Format>,
)> {
	// Read first 6 bytes.
	let mut magic_reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				length: Some(6),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create magic bytes reader"))?;
	let mut magic_bytes = [0u8; 6];
	let bytes_read = magic_reader
		.read(&mut magic_bytes)
		.await
		.map_err(|source| tg::error!(!source, "failed to read magic bytes"))?;

	if bytes_read < 2 {
		return Err(tg::error!("blob too small to be an archive"));
	}

	// .zip
	if magic_bytes[0] == 0x50 && magic_bytes[1] == 0x4B {
		return Ok((tg::artifact::archive::Format::Zip, None));
	}

	// If we can detect a compression format from the magic bytes, assume tar.
	if let Some(compression) = tg::blob::compress::Format::from_magic(&magic_bytes) {
		return Ok((tg::artifact::archive::Format::Tar, Some(compression)));
	}

	// Otherwise, check for uncompressed ustar.
	let mut ustar_reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				position: Some(std::io::SeekFrom::Start(257)),
				length: Some(5),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create ustar reader"))?;
	let mut ustar_bytes = [0u8; 5];
	let ustar_bytes_read_result = ustar_reader.read(&mut ustar_bytes).await.ok();

	// If we successfully read the bytes "ustar" from position 257, it's a tar archive.
	if let Some(bytes_read) = ustar_bytes_read_result {
		if bytes_read == 5 && &ustar_bytes == b"ustar" {
			return Ok((tg::artifact::archive::Format::Tar, None));
		}
	}

	// If not ustar, check for a valid tar checksum.
	if valid_tar_checksum(server, blob).await? {
		return Ok((tg::artifact::archive::Format::Tar, None));
	}

	Err(tg::error!(?magic_bytes, "unrecognized archive format"))
}

async fn valid_tar_checksum(server: &Server, blob: &tg::Blob) -> tg::Result<bool> {
	let mut reader = blob
		.read(
			server,
			tg::blob::read::Arg {
				length: Some(512),
				..Default::default()
			},
		)
		.await
		.map_err(|source| tg::error!(!source, "failed to create the tar header reader"))?;

	let mut header = [0u8; 512];
	let Some(bytes_read) = reader.read(&mut header).await.ok() else {
		// We couldn't read bytes, not a tar archive.
		return Ok(false);
	};
	// We expected to be able to read the full 512 bytes. If not, it's not a tar archive.
	if bytes_read != 512 {
		return Ok(false);
	}

	// Parse the checksum for the header record. This is an 8-byte field at offset 148.
	// See <https://en.wikipedia.org/wiki/Tar_(computing)#File_format>
	// "The checksum is calculated by taking the sum of the unsigned byte values of the header record with the eight checksum bytes taken to be ASCII spaces (decimal value 32). It is stored as a six digit octal number with leading zeroes followed by a NUL and then a space."
	let offset = 148;
	let field_size = 8;
	let recorded_checksum = parse_octal_checksum(&header[offset..offset + field_size])?;

	let mut checksum_calc = 0u32;
	for (i, item) in header.iter().enumerate() {
		// If we're in the checksum field, add ASCII space value.
		if i >= offset && i < offset + field_size {
			checksum_calc += 32;
		} else {
			checksum_calc += u32::from(*item);
		}
	}

	let result = checksum_calc == recorded_checksum;
	Ok(result)
}

fn parse_octal_checksum(bytes: &[u8]) -> tg::Result<u32> {
	// Checksums are stored as octal ASCII digits terminated by a NUL or space.
	let mut checksum_str = String::new();

	for &byte in bytes {
		if byte == 0 || byte == b' ' {
			break;
		}
		checksum_str.push(byte as char);
	}

	// Convert octal string to u32
	match u32::from_str_radix(checksum_str.trim(), 8) {
		Ok(value) => Ok(value),
		Err(_) => Err(tg::error!("Invalid tar checksum format")),
	}
}

impl Artifact {
	fn empty_directory() -> Self {
		Self::Directory {
			id: None,
			data: None,
			entries: BTreeMap::default(),
			count: None,
			depth: None,
			size: None,
			weight: None,
		}
	}

	async fn file(server: &Server, reader: impl AsyncRead, executable: bool) -> tg::Result<Self> {
		// Create the blob, storing to a temp.
		let temp = Temp::new(server);
		let temp_path = temp.path().to_path_buf();
		let destination = Destination::Temp(temp);
		let contents = server.create_blob_inner(reader, Some(&destination)).await?;
		let blob_path = server.blobs_path().join(contents.id.to_string());
		tokio::fs::rename(temp_path, blob_path)
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to rename the file to the blobs directory")
			})?;

		let data = tg::file::Data::Normal {
			contents: contents.id.clone(),
			dependencies: BTreeMap::new(),
			executable,
		};
		let bytes = data.serialize()?;
		let id = tg::file::Id::new(&bytes);
		let count = contents.count + 1;
		let depth = None;
		let size = bytes.len().to_u64().unwrap();
		let weight = contents.weight + size;
		let file = Artifact::File {
			id,
			data,
			contents,
			count,
			size,
			weight,
			depth,
		};
		Ok(file)
	}

	fn symlink(target: impl AsRef<Path>) -> tg::Result<Self> {
		let data = tg::symlink::Data::Target {
			target: target.as_ref().to_path_buf(),
		};
		let bytes = data.serialize()?;
		let id = tg::symlink::Id::new(&bytes);
		let count = 1;
		let depth = None;
		let size = bytes.len().to_u64().unwrap();
		let weight = size;
		let symlink = Artifact::Symlink {
			id,
			data,
			count,
			size,
			weight,
			depth,
		};
		Ok(symlink)
	}

	fn add_entry(&mut self, path: impl AsRef<Path>, artifact: Artifact) -> tg::Result<()> {
		let path = path.as_ref();

		// Ensure we're a directory.
		let Artifact::Directory { depth, entries, .. } = self else {
			return Err(
				tg::error!(?self, %path = path.display(), "can only add entries to directories"),
			);
		};
		let Some(depth) = depth else {
			return Err(tg::error!(?self, "depth not set"));
		};
		let depth = *depth;

		let mut components = path.components();
		let name = loop {
			match components.next() {
				Some(std::path::Component::Normal(name)) => {
					break name;
				},
				Some(std::path::Component::CurDir) => (),
				_ => {
					return Err(
						tg::error!(%path = path.display(), "expected a normal path component"),
					);
				},
			}
		};

		let name = name
			.to_str()
			.ok_or_else(|| tg::error!(?name, "failed to convert name to utf8 string"))?
			.to_string();

		let mut remaining_components = components.peekable();
		if remaining_components.peek().is_some() {
			// Find the parent directory.
			let parent = entries
				.entry(name)
				.or_insert_with(|| Artifact::empty_directory().with_depth(depth + 1));
			if !matches!(parent, Artifact::Directory { .. }) {
				return Err(tg::error!(?parent, "parent path was not a directory"));
			}

			// Insert the artifact.
			let remaining_path: PathBuf = remaining_components.collect();
			parent.add_entry(remaining_path, artifact)?;
		} else {
			// Set the correct depth on the artifact and insert.
			let artifact = artifact.with_depth(depth + 1);
			entries.insert(name, artifact);
		}

		Ok(())
	}

	fn as_artifact(&self) -> tg::Result<tg::Artifact> {
		let id = self.id()?;
		let artifact = tg::Artifact::with_id(id);
		Ok(artifact)
	}

	fn compute_metadata(&mut self) -> tg::Result<()> {
		if let Artifact::Directory {
			id,
			data,
			entries,
			depth,
			count,
			size,
			weight,
		} = self
		{
			// Compute data, recursively computing children as needed.
			let entries_ = entries
				.iter_mut()
				.map(|(name, artifact)| {
					if let Ok(id) = artifact.id() {
						Ok((name.clone(), id))
					} else {
						artifact.compute_metadata()?;
						artifact.id().map(|id| (name.clone(), id))
					}
				})
				.collect::<Result<BTreeMap<String, tg::artifact::Id>, _>>()?;
			let data_ = tg::directory::data::Directory::Normal { entries: entries_ };

			// Compute size.
			let bytes = data_.serialize()?;
			let size_ = bytes.len().to_u64().unwrap();

			// Compute ID.
			let id_ = tg::directory::Id::new(&bytes);

			// Compute metadata.
			let (count_, depth_, weight_) = entries.values().try_fold(
				(1, 1, size_),
				|(acc_count, acc_depth, acc_weight), artifact| {
					let count = artifact.count()?;
					let depth = artifact.depth()?;
					let weight = artifact.weight()?;
					let output = (acc_count + count, acc_depth.max(depth), acc_weight + weight);
					Ok::<_, tg::Error>(output)
				},
			)?;

			// Store values.
			*data = Some(data_);
			*id = Some(id_);
			*count = Some(count_);
			*size = Some(size_);
			*depth = Some(depth_);
			*weight = Some(weight_);
		}
		Ok(())
	}

	fn count(&self) -> tg::Result<u64> {
		let count = match self {
			Artifact::Symlink { count, .. } | Artifact::File { count, .. } => *count,
			Artifact::Directory { count, .. } => {
				let Some(count) = count else {
					return Err(tg::error!(?self, "directory count not yet computed"));
				};
				*count
			},
		};
		Ok(count)
	}

	fn data(&self) -> tg::Result<tg::artifact::Data> {
		let data = match self {
			Artifact::Symlink { data, .. } => data.clone().into(),
			Artifact::File { data, .. } => data.clone().into(),
			Artifact::Directory { data, .. } => {
				let Some(data) = data else {
					return Err(tg::error!(?self, "directory data not yet computed"));
				};
				data.clone().into()
			},
		};
		Ok(data)
	}

	fn depth(&self) -> tg::Result<u64> {
		let depth = match &self {
			Artifact::Symlink { depth, .. }
			| Artifact::File { depth, .. }
			| Artifact::Directory { depth, .. } => {
				let Some(depth) = depth else {
					return Err(tg::error!(?self, "missing depth"));
				};
				*depth
			},
		};
		Ok(depth)
	}

	fn id(&self) -> tg::Result<tg::artifact::Id> {
		let id = match self {
			Artifact::Symlink { id, .. } => id.clone().into(),
			Artifact::File { id, .. } => id.clone().into(),
			Artifact::Directory { id, .. } => {
				let Some(id) = id else {
					return Err(tg::error!(?self, "directory id not yet computed"));
				};
				id.clone().into()
			},
		};
		Ok(id)
	}

	fn weight(&self) -> tg::Result<u64> {
		let weight = match &self {
			Artifact::Symlink { weight, .. } | Artifact::File { weight, .. } => *weight,
			Artifact::Directory { weight, .. } => {
				let Some(weight) = weight else {
					return Err(tg::error!(?self, "directory weight not yet computed"));
				};
				*weight
			},
		};
		Ok(weight)
	}

	fn find(&self, path: impl AsRef<Path>) -> tg::Result<Option<&Artifact>> {
		let path = path.as_ref();

		let Artifact::Directory { entries, .. } = self else {
			return Ok(None);
		};

		let mut components = path.components();
		let name = loop {
			match components.next() {
				Some(std::path::Component::Normal(name)) => {
					break name;
				},
				Some(std::path::Component::CurDir) => (),
				None => {
					return Ok(Some(self));
				},
				_ => {
					return Ok(None);
				},
			}
		};

		let name = name
			.to_str()
			.ok_or_else(|| tg::error!(?name, "failed to convert name to utf8 string"))?;

		let entry = entries
			.get(name)
			.ok_or_else(|| tg::error!(%path = path.display(), "path not found"))?;

		let mut remaining_components = components.peekable();
		if remaining_components.peek().is_some() {
			let remaining_path: PathBuf = remaining_components.collect();
			entry.find(remaining_path)
		} else {
			Ok(Some(entry))
		}
	}

	fn with_depth(self, depth: u64) -> Self {
		let depth = Some(depth);
		match self {
			Self::Directory {
				id,
				data,
				entries,
				count,
				size,
				weight,
				..
			} => Self::Directory {
				id,
				data,
				entries,
				count,
				depth,
				size,
				weight,
			},
			Self::File {
				id,
				data,
				contents,
				count,
				size,
				weight,
				..
			} => Self::File {
				id,
				data,
				contents,
				count,
				depth,
				size,
				weight,
			},
			Self::Symlink {
				id,
				data,
				count,
				size,
				weight,
				..
			} => Self::Symlink {
				id,
				data,
				count,
				depth,
				size,
				weight,
			},
		}
	}

	async fn persist(&mut self, server: &Server) -> tg::Result<()> {
		self.compute_metadata()?;

		let artifact = Arc::new(self.clone());
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();
		let result = futures::try_join!(
			artifact.database_insert(server, touched_at),
			artifact.store_put(server, touched_at)
		);
		if let Err(error) = result {
			return Err(tg::error!(!error, "failed to join futures"));
		}
		Ok(())
	}

	async fn database_insert(self: &Arc<Self>, server: &Server, touched_at: i64) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = server
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let mut transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		match &mut transaction {
			Either::Left(transaction) => {
				transaction
					.with({
						let artifact = self.clone();
						move |transaction| {
							let mut stack = vec![&*artifact];
							while let Some(artifact) = stack.pop() {
								match artifact {
									Artifact::File { contents, .. } => {
										// Write the contents.
										Server::insert_blob_sqlite(contents, transaction)?;
										Server::insert_blob_objects_sqlite(
											contents,
											transaction,
											touched_at,
										)?;
									},
									Artifact::Directory { entries, .. } => {
										// Collect the children.
										stack.extend(entries.values());
									},
									Artifact::Symlink { .. } => {},
								}
								// Write the object.
								write_object_sqlite(artifact, transaction)?;
							}

							Ok::<_, tg::Error>(())
						}
					})
					.await?;
			},
			Either::Right(_) => {
				return Err(tg::error!("unimplemented"));
			},
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	async fn store_put(&self, server: &Server, touched_at: i64) -> tg::Result<()> {
		let mut batch = Vec::new();

		let mut stack = vec![self];
		while let Some(artifact) = stack.pop() {
			let id = artifact.id()?;
			let id: tg::object::Id = id.into();
			let data = artifact.data()?;
			let bytes = data.serialize()?;

			// Add any objects found in the children to the stack.
			match artifact {
				Artifact::File { contents, .. } => {
					let mut contents_batch = Vec::new();
					let mut stack = vec![contents];
					while let Some(blob) = stack.pop() {
						if let Some(data) = &blob.data {
							let id: tg::object::Id = blob.id.clone().into();
							let bytes = data.serialize()?;
							contents_batch.push((id, bytes));
						}
						stack.extend(&blob.children);
					}

					batch.extend_from_slice(&contents_batch);
				},
				Artifact::Directory { entries, .. } => {
					stack.extend(entries.values());
				},
				Artifact::Symlink { .. } => {},
			}
			batch.push((id, bytes));
		}

		let arg = crate::store::PutBatchArg {
			objects: batch,
			touched_at,
		};

		server
			.store
			.put_batch(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to store the objects"))?;
		Ok(())
	}
}

fn write_object_sqlite(
	artifact: &Artifact,
	transaction: &mut rusqlite::Transaction<'_>,
) -> tg::Result<()> {
	let statement = indoc!(
		"
			insert into objects (id, complete, count, depth, incomplete_children, size, touched_at, weight)
			values (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
			on conflict (id) do update set touched_at = ?7;
		"
	);
	let mut statement = transaction
		.prepare_cached(statement)
		.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
	let id = artifact.id()?;
	let data = artifact.data()?;
	let bytes = data.serialize()?;
	let size = bytes.len().to_u64().unwrap();
	let count = artifact.count()?;
	let depth = artifact.depth()?;
	let weight = artifact.weight()?;
	let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
	let params = rusqlite::params![id.to_string(), false, count, depth, 0, size, now, weight,];
	statement
		.execute(params)
		.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
	let statement = indoc!(
		"
			insert into object_children (object, child)
			values (?1, ?2)
			on conflict (object, child) do nothing;
		"
	);
	let mut statement = transaction
		.prepare_cached(statement)
		.map_err(|source| tg::error!(!source, "failed to prepare statement"))?;
	for child in data.children() {
		let params = rusqlite::params![id.to_string(), child.to_string()];
		statement
			.execute(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
	}
	Ok(())
}
