use crate as tg;
use std::{collections::BTreeMap, path::PathBuf};

#[derive(Clone, Debug)]
pub struct State {
	pub cacheable: bool,
	pub checksum: Option<tg::Checksum>,
	pub children: Option<Vec<tg::Process>>,
	pub command: tg::Command,
	pub created_at: time::OffsetDateTime,
	pub cwd: Option<PathBuf>,
	pub dequeued_at: Option<time::OffsetDateTime>,
	pub enqueued_at: Option<time::OffsetDateTime>,
	pub env: Option<BTreeMap<String, String>>,
	pub error: Option<tg::Error>,
	pub exit: Option<tg::process::Exit>,
	pub finished_at: Option<time::OffsetDateTime>,
	pub log: Option<tg::Blob>,
	pub mounts: Vec<tg::process::Mount>,
	pub network: bool,
	pub output: Option<tg::Value>,
	pub retry: bool,
	pub started_at: Option<time::OffsetDateTime>,
	pub status: tg::process::Status,
	pub stderr: Option<tg::pipe::Id>,
	pub stdin: Option<tg::pipe::Id>,
	pub stdout: Option<tg::pipe::Id>,
	pub touched_at: Option<time::OffsetDateTime>,
}

impl TryFrom<tg::process::Data> for tg::process::State {
	type Error = tg::Error;

	fn try_from(value: tg::process::Data) -> Result<Self, Self::Error> {
		let cacheable = value.cacheable;
		let checksum = value.checksum;
		let children = value.children.map(|children| {
			children
				.into_iter()
				.map(|id| tg::Process::new(id, None, None, None, None))
				.collect()
		});
		let command = tg::Command::with_id(value.command);
		let created_at = value.created_at;
		let cwd = value.cwd;
		let dequeued_at = value.dequeued_at;
		let enqueued_at = value.enqueued_at;
		let env = value.env;
		let error = value.error;
		let exit = value.exit;
		let finished_at = value.finished_at;
		let log = value.log.map(tg::Blob::with_id);
		let mounts = value.mounts;
		let network = value.network;
		let output = value.output.map(tg::Value::try_from).transpose()?;
		let retry = value.retry;
		let started_at = value.started_at;
		let status = value.status;
		let stderr = value.stderr;
		let stdin = value.stdin;
		let stdout = value.stdout;
		let touched_at = value.touched_at;
		Ok(State {
			cacheable,
			checksum,
			children,
			command,
			created_at,
			cwd,
			dequeued_at,
			enqueued_at,
			env,
			error,
			exit,
			finished_at,
			log,
			mounts,
			network,
			output,
			retry,
			started_at,
			status,
			stderr,
			stdin,
			stdout,
			touched_at,
		})
	}
}
