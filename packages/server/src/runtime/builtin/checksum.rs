use super::Runtime;
use tangram_client as tg;

impl Runtime {
	pub async fn checksum(&self, process: &tg::Process) -> tg::Result<tg::Value> {
		let server = &self.server;
		let command = process.command(server).await?;

		// Get the args.
		let args = command.args(server).await?;

		// Get the object.
		let object = args
			.get(1)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.clone()
			.try_unwrap_object()
			.ok()
			.ok_or_else(|| tg::error!("expected an object"))?;

		// Get the algorithm.
		let algorithm = args
			.get(2)
			.ok_or_else(|| tg::error!("invalid number of arguments"))?
			.try_unwrap_string_ref()
			.ok()
			.ok_or_else(|| tg::error!("expected a string"))?
			.parse::<tg::checksum::Algorithm>()
			.map_err(|source| tg::error!(!source, "invalid algorithm"))?;

		// Compute the checksum.
		let checksum = if let Ok(artifact) = tg::Artifact::try_from(object.clone()) {
			self.server.checksum_artifact(&artifact, algorithm).await?
		} else if let Ok(blob) = tg::Blob::try_from(object.clone()) {
			self.server.checksum_blob(&blob, algorithm).await?
		} else {
			return Err(tg::error!("invalid object"));
		};

		Ok(checksum.to_string().into())
	}
}
