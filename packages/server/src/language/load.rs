use super::Server;
use include_dir::include_dir;
use tangram_client as tg;

const LIB: include_dir::Dir = include_dir!("$OUT_DIR/lib");

impl Server {
	/// Load a module.
	pub async fn load_module(&self, module: &tg::Module) -> tg::Result<String> {
		match module {
			tg::Module::Dts(module) => {
				let file = LIB
					.get_file(module.path.as_str())
					.ok_or_else(|| tg::error!("expected a file"))?;
				Ok(file.contents_utf8().unwrap().to_owned())
			},
			tg::Module::Artifact(tg::module::Artifact::Id(module)) => {
				Ok(format!(r#"export default tg.Artifact.withId("{module}");"#))
			},
			tg::Module::Directory(tg::module::Directory::Id(module)) => Ok(format!(
				r#"export default tg.Directory.withId("{module}");"#
			)),
			tg::Module::File(tg::module::File::Id(module)) => {
				Ok(format!(r#"export default tg.File.withId("{module}");"#))
			},
			tg::Module::Symlink(tg::module::Symlink::Id(module)) => {
				Ok(format!(r#"export default tg.Symlink.withId("{module}");"#))
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
				file.text(&self.server).await
			},
			tg::Module::Js(tg::module::Js::PackageArtifact(module))
			| tg::Module::Ts(tg::module::Js::PackageArtifact(module)) => {
				tg::Directory::with_id(
					module
						.artifact
						.clone()
						.try_into()
						.ok()
						.ok_or_else(|| tg::error!("expected a directory"))?,
				)
				.get(&self.server, &module.path)
				.await?
				.try_unwrap_file()
				.ok()
				.ok_or_else(|| tg::error!("expected a file"))?
				.text(&self.server)
				.await
			},
			tg::Module::Js(tg::module::Js::PackagePath(_))
			| tg::Module::Ts(tg::module::Js::PackagePath(_)) => self.get_document_text(module).await,
		}
	}
}
