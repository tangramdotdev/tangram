use {tangram_client::prelude::*, usage_rs as usage};

/// Encode an authorization token signed with an all-zero secret, which no server accepts, for tests of how clients handle tokens.
#[derive(Clone, Debug, usage::Args)]
#[usage(args_override_self = false)]
pub struct Args {
	/// The Unix time in seconds at which the token expires.
	#[usage(long)]
	pub expires_at: i64,

	/// A permission that the token grants.
	#[usage(long = "permission", required = true)]
	pub permissions: Vec<tg::authorization::Permission>,

	/// The resource that the token authorizes.
	#[usage(long)]
	pub resource: tg::Id,

	/// The ID of the secret that the token names.
	#[usage(default = "0", long)]
	pub secret_id: u64,
}

pub fn run(args: Args) -> tg::Result<()> {
	let body = tg::authorization::Body {
		expires_at: args.expires_at,
		permissions: args.permissions,
		resource: args.resource,
	};
	let secret = tg::authorization::Secret::new(
		args.secret_id,
		tg::authorization::Algorithm::Blake3,
		[0; 32],
	);
	let token = tg::authorization::Token::sign(body, &secret)?;
	println!("{token}");

	Ok(())
}
