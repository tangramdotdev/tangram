use super::Compiler;
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
				object: tg::module::Object::Path(_path),
				..
			} => {
				todo!()
			},

			tg::Module {
				object: tg::module::Object::Object(object),
				..
			} => {
				// Get the file.
				let object = tg::Object::with_id(object.clone());
				let tg::Object::File(file) = object else {
					return Err(tg::error!("the referrer's object must be a package"));
				};

				// Get the dependency.
				let object = file.get_dependency(&self.server, &import.reference).await?;

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
						tg::import::Kind::Lock => tg::module::Kind::Graph,
						tg::import::Kind::Target => tg::module::Kind::Target,
					}
				} else {
					// If the import has no kind, then infer the kind from the object.
					match &object {
						tg::Object::Leaf(_) => tg::module::Kind::Leaf,
						tg::Object::Branch(_) => tg::module::Kind::Branch,
						tg::Object::Directory(_) => tg::module::Kind::Directory,
						tg::Object::File(file) => match file.module(&self.server).await? {
							Some(tg::file::Module::Js) => tg::module::Kind::Js,
							Some(tg::file::Module::Ts) => tg::module::Kind::Ts,
							_ => tg::module::Kind::File,
						},
						tg::Object::Symlink(_) => tg::module::Kind::Symlink,
						tg::Object::Graph(_) => tg::module::Kind::Graph,
						tg::Object::Target(_) => tg::module::Kind::Target,
					}
				};

				// Create the module.
				let object = object.id(&self.server).await?;
				let module = tg::Module {
					kind,
					object: tg::module::Object::Object(object),
				};

				Ok(module)
			},
		}
	}
}
