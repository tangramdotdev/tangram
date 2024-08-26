use super::Compiler;
use tangram_client as tg;

impl Compiler {
	/// Resolve an import from a module.
	pub async fn resolve_module(
		&self,
		referrer: &tg::module::Reference,
		import: &tg::Import,
	) -> tg::Result<tg::module::Reference> {
		match referrer.source() {
			tg::module::Source::Path(path) => {
				let tg::reference::Path::Path(import) = import.reference.path() else {
					todo!()
				};
				let path = path.clone().join(import.clone());
				let kind = tg::module::Kind::Js;
				let source = path;
				let module = tg::module::Reference::with_kind_and_source(kind, source);
				Ok(module)
			},

			tg::module::Source::Object(object) => {
				// Get the file.
				let object = tg::Object::with_id(object.clone());
				let tg::Object::File(file) = object else {
					return Err(tg::error!("the referrer's object must be a package"));
				};

				// Get the dependency.
				let object = file.get_dependency(&self.server, &import.reference).await?;

				// Determine the kind.
				let kind = if let Some(kind) = import.kind {
					// If the import has a kind, then unconditionally use it.
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
						tg::Object::Directory(_) => todo!(),
						tg::Object::File(_) => tg::module::Kind::File,
						tg::Object::Symlink(_) => tg::module::Kind::Symlink,
						tg::Object::Graph(_) => tg::module::Kind::Graph,
						tg::Object::Target(_) => tg::module::Kind::Target,
					}
				};

				// Create the module reference.
				let object = object.id(&self.server).await?;
				let source = tg::module::Source::Object(object);
				let module = tg::module::Reference::with_kind_and_source(kind, source);

				Ok(module)
			},
		}
	}
}
