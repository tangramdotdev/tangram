use super::{input, object};
use crate::{temp::Temp, Server};
use futures::{stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt as _};
use indoc::formatdoc;
use std::{
	collections::BTreeSet,
	ffi::OsStr,
	os::unix::fs::PermissionsExt as _,
	path::{Path, PathBuf},
	sync::RwLock,
};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use time::format_description::well_known::Rfc3339;

#[derive(Debug)]
pub struct Graph {
	pub nodes: Vec<Node>,
}

#[derive(Clone, Debug)]
pub struct Node {
	pub input: Option<usize>,
	pub id: tg::artifact::Id,
	pub data: tg::artifact::Data,
	pub metadata: tg::object::Metadata,
	pub edges: Vec<Edge>,
}

#[derive(Clone, Debug)]
pub struct Edge {
	pub reference: tg::Reference,
	pub subpath: Option<PathBuf>,
	pub node: usize,
}

struct State {
	nodes: Vec<Node>,
	visited: Vec<Option<usize>>,
}

impl Server {
	pub(super) async fn create_output_graph(
		&self,
		input: &input::Graph,
		object: &object::Graph,
	) -> tg::Result<Graph> {
		// Create the state.
		let state = RwLock::new(State {
			nodes: Vec::with_capacity(object.nodes.len()),
			visited: vec![None; object.nodes.len()],
		});

		// Recurse over the graph.
		self.create_output_graph_inner(input, object, 0, &state)
			.await?;

		// Create the output.
		let output = Graph {
			nodes: state.into_inner().unwrap().nodes,
		};

		Ok(output)
	}

	async fn create_output_graph_inner(
		&self,
		input: &input::Graph,
		object: &object::Graph,
		object_index: usize,
		state: &RwLock<State>,
	) -> tg::Result<usize> {
		// Check if this output node has been visited.
		if let Some(output_index) = state.read().unwrap().visited[object_index] {
			return Ok(output_index);
		}

		// Get the corresponding input node.
		let input_index = object.nodes[object_index]
			.unify
			.object
			.as_ref()
			.left()
			.copied();

		// Create the output node.
		let output_index = {
			let mut state = state.write().unwrap();
			let output_index = state.nodes.len();
			let node = Node {
				input: input_index,
				id: object.nodes[object_index]
					.id
					.clone()
					.unwrap()
					.try_into()
					.unwrap(),
				data: object.nodes[object_index].data.clone().unwrap(),
				metadata: object.nodes[object_index].metadata.clone().unwrap(),
				edges: Vec::new(),
			};
			state.nodes.push(node);
			state.visited[object_index].replace(output_index);
			output_index
		};

		// Create the output edges.
		let edges = object.nodes[object_index]
			.edges
			.iter()
			.map(|edge| {
				Box::pin(self.create_output_graph_inner(input, object, edge.index, state)).map(
					|node| {
						node.map(|node| Edge {
							reference: edge.reference.clone(),
							subpath: edge.subpath.clone(),
							node,
						})
					},
				)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect()
			.await?;

		// Update the output edges.
		state.write().unwrap().nodes[output_index].edges = edges;

		// Return the created node.
		Ok(output_index)
	}

	pub async fn write_output_to_database(&self, output: &Graph) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

		// Get the output in reverse-topological order.
		let mut stack = vec![0];
		let mut visited = vec![false; output.nodes.len()];
		while let Some(output_index) = stack.pop() {
			// Check if we've visited this node yet.
			if visited[output_index] {
				continue;
			}
			visited[output_index] = true;

			// Get the output data.
			let output = &output.nodes[output_index];

			// Write to the database.
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, complete, count, depth, weight, touched_at)
					values ({p}1, {p}2, {p}3, {p}4, {p}5, {p}6, {p}7)
					on conflict (id) do update set touched_at = {p}7;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

			let params: Vec<tangram_database::Value> = db::params![
				output.id,
				output.data.serialize()?,
				output.metadata.complete,
				output.metadata.count,
				output.metadata.depth,
				output.metadata.weight,
				now
			];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| {
					tg::error!(!source, "failed to put the artifact into the database")
				})?;

			// Insert the object's children into the database.
			output
				.data
				.children()
				.iter()
				.map(|child| {
					let id = output.id.clone();
					let transaction = &transaction;
					async move {
						let statement = formatdoc!(
							"
								insert into object_children (object, child)
								values ({p}1, {p}2)
								on conflict (object, child) do nothing;
							"
						);
						let params = db::params![id, child];
						transaction
							.execute(statement, params)
							.await
							.map_err(|source| {
								tg::error!(
									!source,
									"failed to put the object children into the database"
								)
							})
							.ok()
					}
				})
				.collect::<FuturesUnordered<_>>()
				.collect::<Vec<_>>()
				.await;

			stack.extend(output.edges.iter().map(|edge| edge.node));
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	/// Write hardlinks in the cache directory.
	pub async fn write_links(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
	) -> tg::Result<()> {
		let path = self.cache_path().join(output.nodes[node].id.to_string());
		let mut visited = vec![false; output.nodes.len()];
		self.write_links_inner(input, output, node, &path, &mut visited)
			.await?;
		Ok(())
	}

	async fn write_links_inner(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		path: &PathBuf,
		visited: &mut [bool],
	) -> tg::Result<()> {
		// Check if we've visited this node.
		if visited[node] {
			return Ok(());
		};
		visited[node] = true;

		// macOS disables hardlinking for some files within .app dirs, such as *.app/Contents/{PkgInfo,Resources/\*.lproj,_CodeSignature} and .DS_Store. Perform a copy instead in these cases. See <https://github.com/NixOS/nix/blob/95f2b2beabc1ab0447bb4683516598265d71b695/src/libstore/optimise-store.cc#L100>.
		let hardlink_prohibited = if cfg!(target_os = "macos") {
			static APP_DIR_RE: std::sync::LazyLock<regex::Regex> =
				std::sync::LazyLock::new(|| regex::Regex::new(r"\.app/Contents/.+$").unwrap());
			APP_DIR_RE.is_match(path.to_string_lossy().as_ref())
		} else {
			false
		};

		// If this is a file, we need to create a hardlink in the cache directory and create a symlink for its contents in the blobs directory that points to the corresponding entry in the cache directory.
		if let tg::artifact::Data::File(_) = &&output.nodes[node].data {
			// Create hard link to the file or copy as needed.
			let dst = self.cache_path().join(output.nodes[node].id.to_string());
			if hardlink_prohibited {
				let _permit = self.file_descriptor_semaphore.acquire().await.unwrap();
				let result = tokio::fs::copy(path, &dst).await.map(|_| ());
				match result {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(source) => {
						let src = path.display();
						let dst = dst.display();
						return Err(tg::error!(!source, %src, %dst, "failed to copy"));
					},
				}
			} else {
				let result = tokio::fs::hard_link(path, &dst).await;
				match result {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(source) => {
						let src = path.display();
						let dst = dst.display();
						return Err(tg::error!(!source, %src, %dst, "failed to hardlink"));
					},
				}
			};

			// Since every file is a child of a directory we do not need to recurse over file dependencies and can bail early.
			return Ok(());
		}

		// Recurse.
		for edge in &output.nodes[node].edges {
			// Skip any children that are roots.
			let Some(input_index) = output.nodes[edge.node].input else {
				continue;
			};
			if input.nodes[input_index].parent.is_none() {
				continue;
			}

			// Get the path of the dependency.
			let path_ = edge
				.reference
				.item()
				.try_unwrap_path_ref()
				.ok()
				.or_else(|| edge.reference.options()?.path.as_ref())
				.cloned()
				.ok_or_else(|| tg::error!("expected a path dependency"))?;
			let path_ = path.strip_prefix("./").unwrap_or(path_.as_ref());
			// We could use metadata from the input, or the data, this avoids having to acquire a lock.
			let path = if matches!(&output.nodes[node].id, tg::artifact::Id::Directory(_)) {
				path.join(path_)
			} else {
				path.parent().unwrap().join(path_)
			};

			Box::pin(self.write_links_inner(input, output, edge.node, &path, visited)).await?;
		}

		Ok(())
	}
}

impl Server {
	pub(super) async fn copy_or_move_to_cache_directory(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<()> {
		let mut stack = vec![node];
		let mut visited = vec![false; output.nodes.len()];
		while let Some(output_index) = stack.pop() {
			// Check if this node has been visited.
			if visited[output_index] {
				continue;
			}
			visited[output_index] = true;

			// Get the input index.
			let Some(input_index) = output.nodes[output_index].input else {
				continue;
			};
			if input.nodes[input_index].parent.is_none() {
				// Create a temp.
				let temp = Temp::new(self);

				// Copy or move to the temp.
				let mut visited = BTreeSet::new();
				self.copy_or_move_all(
					input,
					output,
					output_index,
					temp.path.clone(),
					&mut visited,
					progress,
				)
				.await?;

				// Update the xattrs and file permissions.
				self.update_xattrs_and_permissions(input, output, output_index, temp.path.clone())
					.await?;

				// Reset the file times to epoch.
				self.set_file_times_to_epoch(&temp.path, true).await?;

				// Rename to the cache directory.
				let dest = self
					.cache_path()
					.join(output.nodes[output_index].id.to_string());
				match tokio::fs::rename(&temp.path, &dest).await {
					Ok(()) => (),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => (),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => (),
					Err(source) => return Err(tg::error!(!source, "failed to rename")),
				}

				// Update hardlinks and xattrs.
				self.write_links(input, output, output_index).await?;

				// Reset the top-level object times to epoch post-rename.
				self.set_file_times_to_epoch(dest, false).await?;
			}
			// Recurse.
			stack.extend(output.nodes[output_index].edges.iter().map(|e| e.node));
		}

		Ok(())
	}

	async fn copy_or_move_all(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		dest: PathBuf,
		visited: &mut BTreeSet<PathBuf>,
		progress: Option<&crate::progress::Handle<tg::artifact::checkin::Output>>,
	) -> tg::Result<()> {
		// Check if we've visited this node.
		let Some(input_index) = output.nodes[node].input else {
			return Ok(());
		};
		let input_node = input.nodes[input_index].clone();
		if visited.contains(&input_node.arg.path) {
			return Ok(());
		}
		visited.insert(input_node.arg.path.clone());

		if input_node.metadata.is_dir() {
			if input_node.arg.destructive {
				match tokio::fs::rename(&input_node.arg.path, &dest).await {
					Ok(()) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENOTEMPTY) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
					Err(source) => {
						return Err(
							tg::error!(!source, %src = input_node.arg.path.display(), %dst = dest.display(), "failed to rename directory"),
						)
					},
				}
			}
			tokio::fs::create_dir_all(&dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create directory"))?;
			let dependencies = output.nodes[node].edges.clone();
			for edge in dependencies {
				let Some(input_index) = output.nodes[edge.node].input else {
					continue;
				};
				let input_node_ = &input.nodes[input_index];
				let diff = input_node_
					.arg
					.path
					.strip_prefix(&input_node.arg.path)
					.unwrap();
				let dest = dest.join(diff);
				Box::pin(self.copy_or_move_all(input, output, edge.node, dest, visited, progress))
					.await?;
			}
		} else if input_node.metadata.is_symlink() {
			let target = self.get_symlink_target(input, output, node).await?;
			tokio::fs::symlink(&target, &dest)
				.await
				.map_err(|source| tg::error!(!source, "failed to create symlink"))?;
		} else if input_node.metadata.is_file() {
			if input_node.arg.destructive {
				match tokio::fs::rename(&input_node.arg.path, &dest).await {
					Ok(()) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::EEXIST) => return Ok(()),
					Err(error) if error.raw_os_error() == Some(libc::ENODEV) => (),
					Err(source) => {
						return Err(
							tg::error!(!source, %path = input_node.arg.path.display(), "failed to rename file"),
						)
					},
				}
			}
			tokio::fs::copy(&input_node.arg.path, &dest).await.map_err(
				|source| tg::error!(!source, %path = input_node.arg.path.display(), %dest = dest.display(), "failed to copy file"),
			)?;
		} else {
			return Err(tg::error!(%path = input_node.arg.path.display(), "invalid file type"));
		}

		Ok(())
	}

	async fn set_file_times_to_epoch(
		&self,
		dest: impl AsRef<Path>,
		recursive: bool,
	) -> tg::Result<()> {
		let dest = dest.as_ref();
		tokio::task::spawn_blocking({
			let dest = dest.to_path_buf();
			move || {
				let mut visited = BTreeSet::new();
				set_file_times_to_epoch_inner(&dest, recursive, &mut visited)?;
				Ok::<_, tg::Error>(())
			}
		})
		.await
		.map_err(|error| {
			tg::error!(
				source = error,
				"failed to set file times to epoch for {:?}",
				dest
			)
		})?
	}

	async fn update_xattrs_and_permissions(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		dest: PathBuf,
	) -> tg::Result<()> {
		let mut visited = vec![false; output.nodes.len()];
		self.update_xattrs_and_permissions_inner(input, output, node, dest, &mut visited)
			.await
	}

	async fn update_xattrs_and_permissions_inner(
		&self,
		input: &input::Graph,
		output: &Graph,
		node: usize,
		dest: PathBuf,
		visited: &mut [bool],
	) -> tg::Result<()> {
		// Check if we've visited this node.
		if visited[node] {
			return Ok(());
		}
		visited[node] = true;

		// If this is a file, write xattrs.
		match &output.nodes[node].data {
			tg::artifact::Data::File(file) => {
				let executable = match &file {
					tg::file::Data::Graph { graph, node } => {
						tg::Graph::with_id(graph.clone()).object(self).await?.nodes[*node]
							.try_unwrap_file_ref()
							.map_err(|_| tg::error!("expected a file"))?
							.executable
					},
					tg::file::Data::Normal { executable, .. } => *executable,
				};

				let permissions = if executable { 0o0755 } else { 0o0644 };
				tokio::fs::set_permissions(&dest, std::fs::Permissions::from_mode(permissions))
					.await
					.map_err(
						|source| tg::error!(!source, %path = dest.display(), "failed to set file permissions"),
					)?;

				let json = serde_json::to_vec(&file)
					.map_err(|source| tg::error!(!source, "failed to serialize file data"))?;

				let metadata = tokio::fs::symlink_metadata(&dest).await.map_err(
					|source| tg::error!(!source, %dest = dest.display(), "failed to get metadata"),
				)?;
				if !metadata.is_file() {
					return Err(tg::error!(%path = dest.display(), "expected a file"));
				}
				xattr::set(&dest, tg::file::XATTR_DATA_NAME, &json).map_err(
					|source| tg::error!(!source, %path = dest.display(), "failed to write file data as an xattr"),
				)?;
				let metadata = serde_json::to_vec(&output.nodes[node].metadata)
					.map_err(|source| tg::error!(!source, "failed to serialize metadata"))?;
				xattr::set(&dest, tg::file::XATTR_METADATA_NAME, &metadata).map_err(
					|source| tg::error!(!source, %path = dest.display(), "failed to write file metadata as an xattr"),
				)?;
			},
			tg::artifact::Data::Directory(_) => {
				let permissions = tokio::fs::metadata(&dest)
					.await
					.map_err(
						|source| tg::error!(!source, %dest = dest.display(), "failed to get metadata"),
					)?
					.permissions()
					.mode();
				let mode = format!("{permissions:o}");
				tokio::fs::set_permissions(&dest, std::fs::Permissions::from_mode(0o0755))
					.await
					.map_err(
						|source| tg::error!(!source, %path = dest.display(), %mode, "failed to set directory permissions"),
					)?;
			},
			tg::artifact::Data::Symlink(_) => (),
		}

		// Recurse over directory entries.
		if !matches!(
			output.nodes[node].data.kind(),
			tg::artifact::Kind::Directory
		) {
			return Ok(());
		}
		let dependencies = output.nodes[node].edges.clone();
		let dependencies = dependencies
			.into_iter()
			.filter_map(|edge| {
				let path = edge
					.reference
					.item()
					.try_unwrap_path_ref()
					.ok()
					.or_else(|| edge.reference.options()?.path.as_ref())?;
				Some((path.clone(), edge.node))
			})
			.collect::<Vec<_>>();

		for (subpath, next_node) in dependencies {
			let subpath = subpath.strip_prefix("./").unwrap_or(subpath.as_ref());
			let dest = dest.join(subpath);
			Box::pin(
				self.update_xattrs_and_permissions_inner(input, output, next_node, dest, visited),
			)
			.await?;
		}

		Ok(())
	}

	async fn get_symlink_target(
		&self,
		input: &input::Graph,
		output: &Graph,
		symlink: usize,
	) -> tg::Result<PathBuf> {
		// Get the artifact and subpath, if they exist..
		let artifact_and_subpath = output.nodes[symlink]
			.edges
			.first()
			.map(|edge| (output.nodes[edge.node].id.clone(), edge.subpath.clone()))
			.or_else(|| {
				let tg::artifact::Data::Symlink(tg::symlink::Data::Artifact { artifact, subpath }) =
					output.nodes[symlink].data.clone()
				else {
					return None;
				};
				Some((artifact, subpath))
			});
		if let Some((artifact, subpath)) = artifact_and_subpath {
			// Find how deep this node is in the output tree.
			let mut input_index = output.nodes[symlink].input.unwrap();
			let mut depth = 0;
			loop {
				let Some(parent) = input.nodes[input_index].parent else {
					break;
				};
				depth += 1;
				input_index = parent;
			}

			// Create the target.
			let id = artifact.to_string();
			let subpath = subpath.unwrap_or_default();

			let components = (0..depth)
				.map(|_| std::path::Component::ParentDir)
				.chain(std::iter::once(std::path::Component::Normal(OsStr::new(
					&id,
				))))
				.chain(subpath.components());

			return Ok(components.collect());
		}

		// Otherwise write the target directly.
		if let tg::artifact::Data::Symlink(tg::symlink::Data::Target { target }) =
			&output.nodes[symlink].data
		{
			return Ok(target.clone());
		}

		Err(tg::error!(?symlink = &output.nodes[symlink], "invalid symlink"))
	}
}

fn set_file_times_to_epoch_inner(
	path: &Path,
	recursive: bool,
	visited: &mut BTreeSet<PathBuf>,
) -> tg::Result<()> {
	if !visited.insert(path.to_owned()) {
		return Ok(());
	}

	let epoch = filetime::FileTime::from_system_time(std::time::SystemTime::UNIX_EPOCH);
	if recursive && path.is_dir() {
		for entry in std::fs::read_dir(path)
			.map_err(|error| tg::error!(source = error, "could not read dir"))?
		{
			let entry =
				entry.map_err(|error| tg::error!(source = error, "could not read entry"))?;
			set_file_times_to_epoch_inner(&entry.path(), recursive, visited)?;
		}
	}

	filetime::set_symlink_file_times(path, epoch, epoch).map_err(|source| {
		tg::error!(
			source = source,
			"failed to set the modified time for {:?}",
			path
		)
	})?;

	Ok(())
}
