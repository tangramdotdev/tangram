use super::Compiler;
use either::Either;
use tangram_client as tg;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::Module,
		import: &tg::Import,
	) -> tg::Result<tg::Module> {
		match referrer {
			tg::Module {
				object: Either::Left(_path),
				..
			} => {
				todo!()
			},

			tg::Module {
				object: Either::Right(object),
				..
			} => {
				// Get the package.
				let object = tg::Object::with_id(object.clone());
				let tg::Object::Package(package) = object else {
					return Err(tg::error!("the referrer's object must be a package"));
				};

				// Get the dependency.
				let object = package
					.get_dependency(&self.server, &import.reference)
					.await?;

				// Determine the kind.
				let kind = if let Some(kind) = import.kind {
					match kind {
						tg::import::Kind::Js => tg::module::Kind::Js,
						tg::import::Kind::Ts => tg::module::Kind::Ts,
						tg::import::Kind::Object => tg::module::Kind::Object,
						tg::import::Kind::Artifact => tg::module::Kind::Artifact,
						tg::import::Kind::Blob => tg::module::Kind::Blob,
						tg::import::Kind::Leaf => tg::module::Kind::Leaf,
						tg::import::Kind::Branch => tg::module::Kind::Branch,
						tg::import::Kind::Directory => tg::module::Kind::Directory,
						tg::import::Kind::File => tg::module::Kind::File,
						tg::import::Kind::Symlink => tg::module::Kind::Symlink,
						tg::import::Kind::Package => tg::module::Kind::Package,
						tg::import::Kind::Target => tg::module::Kind::Target,
					}
				} else {
					// If the import has no kind, then infer the kind from the object.
					match &object {
						tg::Object::Leaf(_) => tg::module::Kind::Leaf,
						tg::Object::Branch(_) => tg::module::Kind::Branch,
						tg::Object::Directory(_) => tg::module::Kind::Directory,
						tg::Object::File(_) => tg::module::Kind::File,
						tg::Object::Symlink(_) => tg::module::Kind::Symlink,
						tg::Object::Package(package) => {
							let metadata = package.metadata(&self.server).await?;
							let kind = metadata.get("kind").ok_or_else(|| {
								tg::error!("kind must be set in package metadata")
							})?;
							let kind = kind
								.try_unwrap_string_ref()
								.ok()
								.ok_or_else(|| tg::error!("invalid kind"))?;
							match kind.as_str() {
								"js" => tg::module::Kind::Js,
								"ts" => tg::module::Kind::Ts,
								_ => {
									return Err(tg::error!("invalid kind"));
								},
							}
						},
						tg::Object::Target(_) => tg::module::Kind::Target,
					}
				};

				// Create the module.
				let object = object.id(&self.server).await?;
				let module = tg::Module {
					kind,
					object: Either::Right(object),
				};

				Ok(module)
			},
		}
	}
}
