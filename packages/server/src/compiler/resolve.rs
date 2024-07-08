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
				package: Either::Left(path),
				..
			} => {
				todo!()
			},

			tg::Module {
				package: Either::Right(package),
				..
			} => {
				// let package = tg::Package::with_id(package.clone());
				// let package = package
				// 	.get_dependency(&self.server, &import.reference)
				// 	.await?;
				// let kind = todo!();
				// let package = package.id(&self.server).await?;
				// let module = tg::Module { kind, package };
				// Ok(module)
				todo!()
			},
		}
	}
}
