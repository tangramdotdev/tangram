use {
	crate::{Index, Request, Response},
	std::time::Duration,
	tangram_client::prelude::*,
};

mod get;
mod key;
mod put;
mod touch;

pub(super) use key::Key;

impl Index {
	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetProcesses {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_process_children_count(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<u64>> {
		let request = tangram_index::read::Request::TryGetProcessChildrenCount { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetProcessChildrenCount(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};
		Ok(output)
	}

	pub async fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		let request = tangram_index::read::Request::TryGetProcessChildren {
			id: id.clone(),
			length,
			position,
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetProcessChildren(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_process_children_and_objects(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::NodeChildren>> {
		let request =
			tangram_index::read::Request::TryGetProcessChildrenAndObjects { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetProcessChildrenAndObjects(output) = response
		else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, tangram_index::process::Process)>> {
		let request = tangram_index::read::Request::TryGetCachedProcesses {
			command: command.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetCachedProcesses(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		if process == ancestor {
			return Ok(true);
		}
		let request = tangram_index::read::Request::ProcessHasAncestor {
			ancestor: ancestor.clone(),
			process: process.clone(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::ProcessHasAncestor(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.touch_processes_inner(ids, None, false, touched_at, time_to_touch)
			.await
	}

	async fn touch_processes_inner(
		&self,
		ids: &[tg::process::Id],
		account: Option<&tangram_index::usage::Account>,
		put_account: bool,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchProcesses(crate::TouchProcesses {
			account: account.cloned(),
			ids: ids.to_vec(),
			put_account,
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Processes(processes) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(processes)
	}

	pub async fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &tangram_index::usage::Account,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.touch_processes_inner(ids, Some(account), true, touched_at, time_to_touch)
			.await
	}

	pub async fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&tangram_index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.touch_processes_inner(ids, account, false, touched_at, time_to_touch)
			.await
	}
}

impl tangram_index::process::Index for Index {
	async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.try_get_processes(ids).await
	}

	async fn try_get_process_children_count(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<u64>> {
		self.try_get_process_children_count(id).await
	}

	async fn try_get_process_children(
		&self,
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
	) -> tg::Result<Option<Vec<tg::process::data::Child>>> {
		self.try_get_process_children(id, position, length).await
	}

	async fn try_get_process_children_and_objects(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tangram_index::process::NodeChildren>> {
		self.try_get_process_children_and_objects(id).await
	}

	async fn try_get_cached_processes(
		&self,
		command: &tg::object::Id,
	) -> tg::Result<Vec<(tg::process::Id, tangram_index::process::Process)>> {
		self.try_get_cached_processes(command).await
	}

	async fn process_has_ancestor(
		&self,
		process: &tg::process::Id,
		ancestor: &tg::process::Id,
	) -> tg::Result<bool> {
		self.process_has_ancestor(process, ancestor).await
	}

	async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.touch_processes(ids, touched_at, time_to_touch).await
	}

	async fn touch_processes_and_put_account(
		&self,
		ids: &[tg::process::Id],
		account: &tangram_index::usage::Account,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.touch_processes_and_put_account(ids, account, touched_at, time_to_touch)
			.await
	}

	async fn touch_processes_with_account(
		&self,
		ids: &[tg::process::Id],
		account: Option<&tangram_index::usage::Account>,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<tangram_index::process::Process>>> {
		self.touch_processes_with_account(ids, account, touched_at, time_to_touch)
			.await
	}
}
