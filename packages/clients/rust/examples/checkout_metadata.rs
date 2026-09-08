use {std::path::PathBuf, tangram_client::prelude::*};

fn main() -> tg::Result<()> {
	let path = std::env::args_os()
		.nth(1)
		.map(PathBuf::from)
		.ok_or_else(|| tg::error!("expected a checkout path"))?;
	let output = tg::file::checkout::read(path)?;
	let output = serde_json::to_string(&output)
		.map_err(|error| tg::error!(!error, "failed to serialize the checkout metadata"))?;
	println!("{output}");
	Ok(())
}
