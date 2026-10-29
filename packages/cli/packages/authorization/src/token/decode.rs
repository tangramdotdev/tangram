use tangram_client::prelude::*;

/// Decode an authorization token without verifying its signature.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[arg(index = 1)]
	pub token: tg::authorization::Token,
}

#[derive(serde::Serialize)]
struct Output {
	expires_at: i64,
	permissions: Vec<tg::authorization::Permission>,
	resource: tg::Id,
	secret_id: u64,
}

pub fn run(args: Args) -> tg::Result<()> {
	let tg::authorization::Token {
		body: tg::authorization::Body {
			expires_at,
			permissions,
			resource,
		},
		secret_id,
		..
	} = args.token;
	let output = Output {
		expires_at,
		permissions,
		resource,
		secret_id,
	};
	let output = serde_json::to_string(&output)
		.map_err(|error| tg::error!(!error, "failed to serialize the output"))?;
	println!("{output}");

	Ok(())
}
