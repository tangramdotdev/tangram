use {crate::Index, tangram_client::prelude::*};

mod delete;
mod get;
mod key;
mod list;
mod processes;
mod put;

pub(super) use key::Key;

impl Index {
	pub async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, tangram_index::sandbox::Sandbox)>> {
		let request = tangram_index::read::Request::ListSandboxesForCreator {
			creator: creator.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::ListSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, tangram_index::sandbox::Sandbox)>> {
		let request = tangram_index::read::Request::ListSandboxesForOwner {
			owner: owner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::ListSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		let request = tangram_index::read::Request::GetRunnerSandboxes {
			runner: runner.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::GetRunnerSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_sandbox_processes_count(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		let request = tangram_index::read::Request::TryGetSandboxProcessesCount { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetSandboxProcessesCount(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};
		Ok(output)
	}

	pub async fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::Id>>> {
		let request = tangram_index::read::Request::TryGetSandboxProcesses {
			id: id.clone(),
			length,
			position,
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetSandboxProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn list_sandboxes(
		&self,
	) -> tg::Result<Vec<(tg::sandbox::Id, tangram_index::sandbox::Sandbox)>> {
		let response = self
			.send_read_request(tangram_index::read::Request::ListSandboxes)
			.await?;
		let tangram_index::read::Response::ListSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<tangram_index::sandbox::Sandbox>>> {
		let request = tangram_index::read::Request::TryGetSandboxes {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetSandboxes(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}
}

impl tangram_index::sandbox::Index for Index {
	async fn list_sandboxes_for_creator(
		&self,
		creator: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, tangram_index::sandbox::Sandbox)>> {
		self.list_sandboxes_for_creator(creator).await
	}

	async fn list_sandboxes_for_owner(
		&self,
		owner: &tg::Principal,
	) -> tg::Result<Vec<(tg::sandbox::Id, tangram_index::sandbox::Sandbox)>> {
		self.list_sandboxes_for_owner(owner).await
	}

	async fn get_runner_sandboxes(
		&self,
		runner: &tg::runner::Id,
	) -> tg::Result<Vec<tg::sandbox::Id>> {
		self.get_runner_sandboxes(runner).await
	}

	async fn try_get_sandbox_processes_count(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<u64>> {
		self.try_get_sandbox_processes_count(id).await
	}

	async fn try_get_sandbox_processes(
		&self,
		id: &tg::sandbox::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::Id>>> {
		self.try_get_sandbox_processes(id, position, length).await
	}

	async fn list_sandboxes(
		&self,
	) -> tg::Result<Vec<(tg::sandbox::Id, tangram_index::sandbox::Sandbox)>> {
		self.list_sandboxes().await
	}

	async fn try_get_sandboxes(
		&self,
		ids: &[tg::sandbox::Id],
	) -> tg::Result<Vec<Option<tangram_index::sandbox::Sandbox>>> {
		self.try_get_sandboxes(ids).await
	}
}
