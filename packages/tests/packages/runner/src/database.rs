use {crate::Result, std::process::ExitCode};

pub use tangram_tests_util::database::*;

/// Manage the database pool for the tests.
#[derive(clap::Subcommand)]
pub enum Subcommand {
	/// Lease a database pool instance for the test's run, record it in the test's temporary directory, and print its name.
	Acquire,

	/// Print the environment variables that let tests use the cloud databases, or nothing if the databases are not ready.
	Env,

	/// Provision database pool instances until the pool has the count.
	Provision {
		#[arg(long)]
		count: usize,

		/// The number of instances to create at once.
		#[arg(long)]
		workers: usize,
	},
}

pub async fn command(subcommand: Subcommand) -> Result<ExitCode> {
	match subcommand {
		Subcommand::Acquire => {
			let run = std::env::var("TANGRAM_TEST_DATABASE_RUN")
				.map_err(|_| "TANGRAM_TEST_DATABASE_RUN is not set")?;
			let instance = acquire(&pool_path(), &run).await?;
			record(&instance)?;
			println!("{instance}");
		},
		Subcommand::Env => {
			if let Err(error) = check().await {
				eprintln!("{error}");
				eprintln!("the tests that require the cloud databases will be skipped");
				return Ok(ExitCode::SUCCESS);
			}
			println!("TANGRAM_TEST_CLOUD=1");
			println!("TANGRAM_TEST_DATABASE_POOL={}", pool_path().display());
			println!("TANGRAM_TEST_FDB_CLUSTER={}", cluster_path()?.display());
		},
		Subcommand::Provision { count, workers } => {
			provision(&pool_path(), count, workers).await?;
		},
	}
	Ok(ExitCode::SUCCESS)
}
