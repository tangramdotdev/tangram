use {
	tangram_client::prelude::*,
	tangram_server_authentication_common::token,
	tangram_tests_util::{Config, Temp, cli::Server},
};

/// A process and a sandbox can read their own nodes without authorization searches, and only their own.
#[tokio::test]
async fn principal_self() -> tg::Result<()> {
	// Start a server whose authentication secret the test knows, and which does not verify permissions.
	let temp = Temp::new().map_err(|error| tg::error!(!error, "failed to create the directory"))?;
	let secret_path = temp.path().join("secret");
	let bytes = [7; 32];
	std::fs::write(&secret_path, bytes)
		.map_err(|error| tg::error!(!error, "failed to write the secret"))?;
	let secret = tg::authorization::Secret::new(0, tg::authorization::Algorithm::HmacSha256, bytes);
	let root_token = "root";
	let mut config = Config::new();
	config.merge(serde_json::json!({
		"authentication": {
			"root": { "token": root_token },
			"tokens": { "secret": { "algorithm": "hmac_sha256", "id": 0, "path": secret_path } },
			"users": { "providers": { "insecure": true } },
		},
		"verification": { "permissions": { "initial": false, "final": false } },
	}));
	let server = Server::start(env!("CARGO_BIN_EXE_tangram"), config).await?;

	// Build two processes, each in its own sandbox.
	let package =
		Temp::new().map_err(|error| tg::error!(!error, "failed to create the package"))?;
	std::fs::write(
		package.path().join("tangram.ts"),
		"export default () => 42;",
	)
	.map_err(|error| tg::error!(!error, "failed to write the module"))?;
	let path = package
		.path()
		.to_str()
		.ok_or_else(|| tg::error!("invalid path"))?;
	let (process, sandbox) = build(
		&server,
		root_token,
		&["build", "--detach", "--no-tokens", path],
	)
	.await?;
	let (other, other_sandbox) = build(
		&server,
		root_token,
		&["build", "--cached=false", "--detach", "--no-tokens", path],
	)
	.await?;
	assert_ne!(process, other);
	assert_ne!(sandbox, other_sandbox);

	// Sign authentication tokens for the first process and its sandbox.
	let process_token = sign(&secret, token::Principal::Process(process.parse()?))?;
	let sandbox_token = sign(&secret, token::Principal::Sandbox(sandbox.parse()?))?;

	// The process and the sandbox can read their own nodes, and the sandbox can read its process.
	for (token, args, message) in [
		(
			&process_token,
			["process", "get", process.as_str()],
			"a process should read its own node",
		),
		(
			&sandbox_token,
			["sandbox", "get", sandbox.as_str()],
			"a sandbox should read its own node",
		),
		(
			&sandbox_token,
			["process", "get", process.as_str()],
			"a sandbox should retain access to its processes",
		),
	] {
		let output = server.output(Some(token), args).await?;
		assert!(output.status.success(), "{message}: {output:?}");
	}

	// They cannot read a different process or sandbox.
	for (token, args, message) in [
		(
			&process_token,
			["process", "get", other.as_str()],
			"a process should not read another process",
		),
		(
			&sandbox_token,
			["sandbox", "get", other_sandbox.as_str()],
			"a sandbox should not read another sandbox",
		),
		(
			&sandbox_token,
			["process", "get", other.as_str()],
			"a sandbox should not read another sandbox's process",
		),
	] {
		let output = server.output(Some(token), args).await?;
		assert!(!output.status.success(), "{message}");
	}

	server.stop().await?;

	Ok(())
}

/// Build the package with the arguments, wait for the process, and return it and its sandbox.
async fn build(server: &Server, token: &str, args: &[&str]) -> tg::Result<(String, String)> {
	let process = server.tg(Some(token), args).await?;
	let process = process.split('?').next().unwrap_or(&process).to_owned();
	server.tg(Some(token), ["wait", process.as_str()]).await?;
	let data = server
		.get::<tg::process::Data>(Some(token), &process)
		.await?;
	let sandbox = data
		.sandbox
		.ok_or_else(|| tg::error!("expected the process's sandbox"))?
		.to_string();
	Ok((process, sandbox))
}

/// Sign an authentication token for the principal that expires in a minute.
fn sign(secret: &tg::authorization::Secret, principal: token::Principal) -> tg::Result<String> {
	let now = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_err(|error| tg::error!(!error, "failed to get the time"))?
		.as_secs();
	let body = token::Body {
		expires_at: i64::try_from(now).unwrap() + 60,
		principal,
	};
	let signed = token::Signed::sign(body, secret)?;
	Ok(token::Token::Signed(signed).to_string())
}
