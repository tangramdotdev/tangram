use {crate::Server, tangram_client as tg};

impl Server {
	pub async fn run_builtin_checksum(
		&self,
		process: &tg::Process,
	) -> tg::Result<crate::run::Output> {
		let command = process.command(self).await?;

		// Get the args.
		let args = command.args(self).await?;

		// Get the object.
		let object = args
			.first()
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?;

		// Get the algorithm.
		let algorithm = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::checksum::Algorithm>()
			.map_err(|source| tg::error!(!source, "invalid algorithm"))?;

		// Compute the checksum.
		let checksum = if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			self.checksum_blob(&blob, algorithm).await?
		} else if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			self.checksum_artifact(&artifact, algorithm).await?
		} else {
			return Err(tg::error!("invalid object"));
		};

		let output = checksum.to_string().into();

		let output = crate::run::Output {
			checksum: None,
			error: None,
			exit: 0,
			output: Some(output),
		};

		Ok(output)
	}
}
