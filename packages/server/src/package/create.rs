use crate::Server;
use either::Either;
use std::collections::{BTreeMap, BTreeSet};
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
mod graph;

impl Server {
	pub async fn create_package(
		&self,
		arg: tg::package::create::Arg,
	) -> tg::Result<tg::package::create::Output> {
		let tg::package::create::Arg {
			reference,
			locked,
			remote,
		} = arg;

		// Handle the remote.
		if let Some(remote) = remote.as_ref() {
			let remote = self
				.remotes
				.get(remote)
				.ok_or_else(|| tg::error!(%remote, "remote does not exist"))?;
			let mut reference = reference.clone();
			if let Some(query) = &mut reference.query {
				query.remote.take();
			}
			let arg = tg::package::create::Arg {
				reference,
				locked: false,
				remote: None,
			};
			return remote.create_package(arg).await;
		};

		// Get the path or object that needs to be resolved.
		let root = if reference
			.query
			.as_ref()
			.map_or(true, |q| q.remote.is_some())
		{
			// If the reference specifies a remote, short circuit and resolve path dependencies.
			let object = self
				.get_object_tags(&reference)
				.await?
				.first()
				.ok_or_else(|| tg::error!("expected an object"))?
				.clone()
				.1;
			Either::Right(object)
		} else if let tg::reference::Path::Object(object) = &reference.path {
			// If the dependency specifies an object, create it and resolve path dependencies.
			let object = tg::Object::with_id(object.clone());
			Either::Right(object)
		} else if let tg::reference::Path::Path(path) = &reference.path {
			// If the dependency specifies an object, create it and resolve path dependencies.
			Either::Left(path.clone())
		} else {
			// Otherwise, try and lookup locally.
			let object = self
				.get_object_tags(&reference)
				.await?
				.first()
				.ok_or_else(|| tg::error!("expected an object"))?
				.clone()
				.1;
			Either::Right(object)
		};

		// Resolve path dependencies to create the initial package object.
		let package = self.create_package_with_path_dependencies(root).await?;

		let package = 'a: {
			// If the caller asked for a remote dependency don't try and read one from disk.
			if reference
				.query
				.as_ref()
				.map_or(false, |q| q.remote.is_some())
			{
				break 'a package;
			}

			// Read an existing tangram.lock if it exists.
			let tg::reference::Path::Path(path) = &reference.path else {
				break 'a package;
			};
			let Some(existing_package) = self.try_read_lockfile(path).await? else {
				break 'a package;
			};

			// Add the path dependencies to the lock. This returns None if the packages don't match.
			if let Some(package) = self
				.try_add_path_references_to_package(&package, &existing_package)
				.await?
			{
				break 'a package;
			}

			// Return an error if the packages don't match.
			if locked {
				return Err(tg::error!("tangram.lock is out of date"));
			}

			// Fall back to the newly created package.
			package
		};

		// Resolve the remote dependencies.
		let package = self
			.create_package_with_repository_dependencies(&package, &reference)
			.await?;

		// Remove path dependencies.
		let package = self
			.remove_path_references_from_package(&package, &reference)
			.await?;

		// Create the output.
		let output = tg::package::create::Output {
			package: package.id(self).await?,
		};

		Ok(output)
	}

	// Create a package and resolve all of its path dependencies.
	pub async fn create_package_with_path_dependencies(
		&self,
		root: Either<tg::Path, tg::Object>,
	) -> tg::Result<tg::Package> {
		let parent = match root {
			// If the package is at a path, use the path as the parent.
			Either::Left(path) => Either::Left(path.clone()),

			// If the package is a directory, use the directory object as a parent.
			Either::Right(tg::Object::Directory(directory)) => Either::Right(directory.clone()),

			// If the package refers to a package, short circuit.
			Either::Right(tg::Object::Package(package)) => {
				return Ok(package.clone());
			},

			// Otherwise create a package with unresolved path dependencies.
			Either::Right(object) => {
				let node = self.create_package_node_with_object(&object).await?;
				let nodes = vec![node];
				let root = 0;
				return Ok(tg::Package::with_object(tg::package::Object {
					root,
					nodes,
				}));
			},
		};

		// Create the state.
		let mut nodes = vec![];
		let child = ".".into();
		let mut visited = BTreeMap::new();

		// Walk the package tree.
		self.create_package_with_path_dependencies_inner(&parent, child, &mut nodes, &mut visited)
			.await?;

		// Create the object.
		let object = tg::package::Object { root: 0, nodes };
		Ok(tg::Package::with_object(object))
	}

	async fn create_package_with_path_dependencies_inner(
		&self,
		parent: &Either<tg::Path, tg::Directory>,
		child: tg::Path,
		nodes: &mut Vec<tg::package::Node>,
		visited: &mut BTreeMap<Either<tg::Path, tg::object::Id>, usize>,
	) -> tg::Result<usize> {
		// Create a new node, and get its key into the visited table.
		let (key, node) = match &parent {
			Either::Left(parent_path) => {
				let path = parent_path.clone().join(child.clone()).normalize();
				let node = self.create_package_node_with_path(&path).await?;
				let key = Either::Left(path);
				(key, node)
			},
			Either::Right(parent) => {
				let object = parent.get(self, &child).await?.into();
				let node = self.create_package_node_with_object(&object).await?;
				let key = Either::Right(object.id(self).await?);
				(key, node)
			},
		};

		// If this node has already been visited, return its index.
		if let Some(index) = visited.get(&key) {
			return Ok(*index);
		}

		// Insert this node into the visited nodes table, and the list of all nodes.
		let index = nodes.len();
		visited.insert(key, index);
		nodes.push(node);
		// Collect the path dependencies.
		let mut path_dependencies = nodes[index]
			.dependencies
			.iter()
			.filter_map(|(reference, package)| {
				(package.is_none() && reference.path.try_unwrap_path_ref().is_ok())
					.then_some(reference.clone())
			})
			.collect::<Vec<_>>();

		// Sort to ensure the graph is traversed in the same order.
		path_dependencies.sort_unstable();

		// Recurse.
		for reference in path_dependencies {
			let child = child
				.clone()
				.join(reference.path.try_unwrap_path_ref().unwrap().clone());
			let child_index = Box::pin(
				self.create_package_with_path_dependencies_inner(&parent, child, nodes, visited),
			)
			.await?;
			nodes[index]
				.dependencies
				.get_mut(&reference)
				.unwrap()
				.replace(Either::Left(child_index));
		}

		Ok(index)
	}

	async fn create_package_node_with_path(
		&self,
		path: &tg::Path,
	) -> tg::Result<tg::package::Node> {
		let artifact = tg::Artifact::check_in(
			self,
			tg::artifact::checkin::Arg {
				path: path.clone(),
				destructive: false,
			},
		)
		.await
		.map_err(|source| tg::error!(!source, %path, "failed to check in the artifact"))?
		.into();
		self.create_package_node_with_object(&artifact).await
	}

	async fn create_package_node_with_object(
		&self,
		object: &tg::Object,
	) -> tg::Result<tg::package::Node> {
		let dependencies = self.get_references_for_object(&object).await?;
		let metadata = BTreeMap::default();
		Ok(tg::package::Node {
			dependencies,
			metadata,
			object: Some(object.clone()),
		})
	}

	async fn get_references_for_object(
		&self,
		object: &tg::Object,
	) -> tg::Result<BTreeMap<tg::Reference, Option<Either<usize, tg::Package>>>> {
		match object {
			tg::Object::File(file) => self.get_references_for_file(file).await,
			tg::Object::Directory(directory) => self.get_references_for_directory(directory).await,
			tg::Object::Symlink(_) => Err(tg::error!("Cannot create a package from a symlink")),
			_ => Ok(BTreeMap::new()),
		}
	}

	async fn get_references_for_file(
		&self,
		file: &tg::File,
	) -> tg::Result<BTreeMap<tg::Reference, Option<Either<usize, tg::Package>>>> {
		let text = file.text(self).await?;
		let Ok(analysis) = crate::compiler::Compiler::analyze_module(text) else {
			return Ok(BTreeMap::new());
		};
		let references = analysis
			.imports
			.into_iter()
			.map(|import| (import.reference, None))
			.collect();
		Ok(references)
	}

	async fn get_references_for_directory(
		&self,
		directory: &tg::Directory,
	) -> tg::Result<BTreeMap<tg::Reference, Option<Either<usize, tg::Package>>>> {
		let Some(root_module_path) =
			tg::package::module::try_get_root_module_path(self, &directory.clone().into()).await?
		else {
			return Ok(BTreeMap::new());
		};
		let module = directory.get(self, &root_module_path).await?;
		match module {
			tg::Artifact::File(file) => self.get_references_for_file(&file).await,
			tg::Artifact::Directory(_) => {
				return Err(tg::error!(
					"expected the root module to be a file or symlink"
				))
			},
			tg::Artifact::Symlink(symlink) => {
				Box::pin(self.get_references_for_symlink(&symlink)).await
			},
		}
	}

	async fn get_references_for_symlink(
		&self,
		symlink: &tg::Symlink,
	) -> tg::Result<BTreeMap<tg::Reference, Option<Either<usize, tg::Package>>>> {
		let Some(resolved) = symlink.resolve(self).await? else {
			return Ok(BTreeMap::new());
		};
		match resolved {
			tg::Artifact::File(file) => self.get_references_for_file(&file).await,
			tg::Artifact::Directory(directory) => {
				self.get_references_for_directory(&directory).await
			},
			_ => unreachable!(),
		}
	}

	async fn try_read_lockfile(&self, path: &tg::Path) -> tg::Result<Option<tg::Package>> {
		let path = path.clone().join(tg::package::LOCKFILE_FILE_NAME);
		let bytes = match tokio::fs::read(&path).await {
			Ok(bytes) => bytes,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
				return Ok(None);
			},
			Err(source) => {
				return Err(tg::error!(!source, %path, "failed to read the lockfile"));
			},
		};
		let data = tg::package::Data::deserialize(&bytes.into())?;
		let object: tg::package::Object = data.try_into()?;
		let package = tg::Package::with_object(object);
		Ok(Some(package))
	}

	async fn create_package_with_repository_dependencies(
		&self,
		package: &tg::Package,
		reference: &tg::Reference,
	) -> tg::Result<tg::Package> {
		let (root, graph) = self
			.create_package_graph(package, reference)
			.await
			.map_err(|source| tg::error!(!source, "failed to create package graph"))?;
		let package = graph.into_package_object(&root);
		Ok(tg::Package::with_object(package))
	}

	async fn try_add_path_references_to_package(
		&self,
		left: &tg::Package,
		right: &tg::Package,
	) -> tg::Result<Option<tg::Package>> {
		let left = left.object(self).await?;
		let mut right = right.object(self).await?.as_ref().clone();
		let mut stack = vec![right.root];
		let mut visited = BTreeSet::new();
		while let Some(index) = stack.pop() {
			if visited.contains(&index) {
				continue;
			}
			visited.insert(index);

			let Some(object) = left.nodes[index].object.clone() else {
				return Ok(None);
			};
			right.nodes[index].object.replace(object);
			let neighbors = left.nodes[index]
				.dependencies
				.values()
				.zip(right.nodes[index].dependencies.values_mut());

			for neighbor in neighbors {
				let (Some(left), Some(right)) = neighbor else {
					return Ok(None);
				};
				match (left, right) {
					(Either::Left(_), Either::Left(right)) => {
						stack.push(*right);
					},
					(Either::Right(left), Either::Right(right)) => {
						let Some(package) =
							Box::pin(self.try_add_path_references_to_package(left, right)).await?
						else {
							return Ok(None);
						};
						*right = package;
					},
					_ => return Ok(None),
				}
			}
		}
		Ok(Some(tg::Package::with_object(right)))
	}

	async fn remove_path_references_from_package(
		&self,
		package: &tg::Package,
		reference: &tg::Reference,
	) -> tg::Result<tg::Package> {
		let mut object = package.object(self).await?.as_ref().clone();
		let mut stack = vec![(object.root, reference.clone())];
		let mut visited = BTreeSet::new();
		while let Some((index, reference)) = stack.pop() {
			if visited.contains(&index) || reference.query.map_or(true, |q| q.path.is_none()) {
				continue;
			}
			visited.insert(index);
			object.nodes[index].object.take();
			for (reference, package) in &mut object.nodes[index].dependencies {
				let Some(package) = package else {
					continue;
				};
				match package {
					Either::Left(index) => stack.push((*index, reference.clone())),
					Either::Right(package) => {
						*package =
							Box::pin(self.remove_path_references_from_package(package, reference))
								.await?;
					},
				}
			}
		}
		Ok(tg::Package::with_object(object))
	}

	async fn get_object_tags(
		&self,
		reference: &tg::Reference,
	) -> tg::Result<Vec<(tg::Tag, tg::Object)>> {
		// Get the tag pattern and remote if necessary.
		let pattern = reference
			.path
			.try_unwrap_tag_ref()
			.map_err(|_| tg::error!(%reference, "expected a tag pattern"))?
			.clone();
		let remote = reference
			.query
			.as_ref()
			.and_then(|query| query.remote.clone());

		// List tags that match the pattern.
		let output = self
			.list_tags(tg::tag::list::Arg {
				pattern: pattern.clone(),
				remote,
			})
			.await
			.map_err(|source| tg::error!(!source, %pattern, "failed to get tags"))?;

		// Convert the tag objects into packages.
		Ok(output
			.data
			.into_iter()
			.filter_map(|output| {
				let object = output.item.right()?;
				Some((output.tag, tg::Object::with_id(object)))
			})
			.collect())
	}
}

impl Server {
	pub(crate) async fn handle_create_package_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.create_package(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
