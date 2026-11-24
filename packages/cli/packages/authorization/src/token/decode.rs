use {tangram_client::prelude::*, usage_rs as usage};

/// Decode an authorization token without verifying its signature.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
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
