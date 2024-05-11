use super::Server;
use include_dir::include_dir;
use tangram_client as tg;

const LIB: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Server {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> tg::Result<String> {
		match module {
			tg::Module::Js(tg::module::Js::File(module))
			| tg::Module::Ts(tg::module::Js::File(module)) => {
				let artifact = tg::Artifact::with_id(module.clone());
				let file = match artifact {
					tg::Artifact::File(file) => file,
					tg::Artifact::Symlink(symlink) => symlink
						.resolve(&self.server)
						.await?
						.ok_or_else(|| tg::error!("failed to resolve symlink"))?
						.try_unwrap_file()
						.ok()
						.ok_or_else(|| tg::error!("expected a file"))?,
					tg::Artifact::Directory(_) => return Err(tg::error!("expected a directory")),
				};
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			tg::Module::Js(tg::module::Js::PackageArtifact(module))
			| tg::Module::Ts(tg::module::Js::PackageArtifact(module)) => {
				let id = module
					.artifact
					.clone()
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("expected a directory"))?;
				let directory = tg::Directory::with_id(id);
				let file = directory
					.get(&self.server, &module.path)
					.await?
					.try_unwrap_file()
					.ok()
					.ok_or_else(|| tg::error!("expected a file"))?;
				let text = file.text(&self.server).await?;
				Ok(text)
			},

			tg::Module::Js(tg::module::Js::PackagePath(_))
			| tg::Module::Ts(tg::module::Js::PackagePath(_)) => self.get_document_text(module).await,

			tg::Module::Dts(module) => {
				let file = LIB
					.get_file(module.path.as_str())
					.ok_or_else(|| tg::error!("expected a file"))?;
				let text = file.contents_utf8().unwrap().to_owned();
				Ok(text)
			},

			tg::Module::Artifact(tg::module::Artifact::Id(id)) => {
				Ok(format!(r#"export default tg.Artifact.withId("{id}");"#))
			},

			tg::Module::Directory(tg::module::Directory::Id(id)) => {
				Ok(format!(r#"export default tg.Directory.withId("{id}");"#))
			},

			tg::Module::File(tg::module::File::Id(id)) => {
				Ok(format!(r#"export default tg.File.withId("{id}");"#))
			},

			tg::Module::Symlink(tg::module::Symlink::Id(id)) => {
				Ok(format!(r#"export default tg.Symlink.withId("{id}");"#))
			},

			tg::Module::Artifact(tg::module::Artifact::Path(_)) => {
				Ok("export default undefined as tg.Artifact;".into())
			},

			tg::Module::Directory(tg::module::Directory::Path(_)) => {
				Ok("export default undefined as tg.Directory;".into())
			},

			tg::Module::File(tg::module::File::Path(_)) => {
				Ok("export default undefined as tg.File;".into())
			},

			tg::Module::Symlink(tg::module::Symlink::Path(_)) => {
				Ok("export default undefined as tg.Symlink;".into())
			},
		}
	}
}
