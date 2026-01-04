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
		let tg = worktree
			.which("tangram")
			.ok_or("failed to find the tangram binary")?;
		let command = zed::Command {
			command: tg,
			args: vec!["lsp".to_owned()],
			env: vec![],
		};
		Ok(command)
	}
}

zed::register_extension!(Extension);
