use {tangram_cli_core::Cli, tangram_client::prelude::*};

/// Get the server's status.
#[derive(Clone, Debug, clap::Args)]
#[group(skip)]
pub struct Args {
	#[command(flatten)]
	pub print: tangram_cli_core::print::Options,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct Output {
	#[serde(skip_serializing_if = "Option::is_none")]
	pub manager: Option<tg::server::Manager>,

	#[serde(skip_serializing_if = "Option::is_none")]
	pub pid: Option<u32>,

	pub status: Status,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
	Started,
	Stopped,
}

pub async fn run(cli: &mut Cli, args: Args) -> tg::Result<()> {
	// Get the status.
	let client = cli.create_client()?;
	let status = if client.connect().await.is_ok() {
		Status::Started
	} else {
		Status::Stopped
	};

	// Read the manager and the pid from the lock file of a started server.
	let lock = match status {
		Status::Started => cli.try_read_server_lock().await?,
		Status::Stopped => None,
	};

	// Print the output.
	let manager = lock.as_ref().and_then(|lock| lock.manager);
	let pid = lock.as_ref().map(|lock| lock.pid);
	let output = Output {
		manager,
		pid,
		status,
	};
	cli.print_serde(output, args.print).await?;

	Ok(())
}
