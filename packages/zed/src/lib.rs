use zed_extension_api::{self as zed, Result};

struct Extension;

impl zed::Extension for Extension {
	fn new() -> Self {
		Self
	}

	fn language_server_command(
		&mut self,
		_language_server_id: &zed::LanguageServerId,
		worktree: &zed::Worktree,
	) -> Result<zed::Command> {
		let path = worktree
			.which("tangram")
			.ok_or("failed to find the tangram binary")?;
		Ok(zed::Command {
			command: path,
			args: vec!["lsp".to_owned()],
			env: vec![],
		})
	}
}

zed::register_extension!(Extension);
