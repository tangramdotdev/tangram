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
				todo!()
			},
		}
	}
}
