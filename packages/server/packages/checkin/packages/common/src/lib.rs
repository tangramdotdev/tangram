use {indoc::indoc, tangram_client::prelude::*};

mod lock;

pub use self::lock::strip_lock;

/// Create the ignorer for check-ins.
pub fn create_ignorer() -> tg::Result<tangram_ignore::Ignorer> {
	let file_names = vec![".tangramignore".into(), ".gitignore".into()];
	let global = indoc!(
		"
			.DS_Store
			.git
			.tangram
			tangram.lock
		"
	);
	tangram_ignore::Ignorer::new(file_names, Some(global))
		.map_err(|error| tg::error!(!error, "failed to create the matcher"))
}
