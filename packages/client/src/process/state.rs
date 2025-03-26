use crate as tg;

#[derive(Clone, Debug)]
pub struct State {
	pub cacheable: bool,
	pub checksum: Option<tg::Checksum>,
	pub children: Option<Vec<tg::Process>>,
	pub command: tg::Command,
	pub created_at: time::OffsetDateTime,
	pub dequeued_at: Option<time::OffsetDateTime>,
	pub enqueued_at: Option<time::OffsetDateTime>,
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
	pub stderr: Option<tg::process::Stdio>,
	pub stdin: Option<tg::process::Stdio>,
	pub stdout: Option<tg::process::Stdio>,
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
		let dequeued_at = value.dequeued_at;
		let enqueued_at = value.enqueued_at;
		let error = value.error;
		let exit = value.exit;
		let finished_at = value.finished_at;
		let log = value.log.map(tg::Blob::with_id);
		let mounts = value.mounts.into_iter().map(Into::into).collect();
		let network = value.network;
		let output = value.output.map(tg::Value::try_from).transpose()?;
		let retry = value.retry;
		let started_at = value.started_at;
		let status = value.status;
		let stderr = value.stderr;
		let stdin = value.stdin;
		let stdout = value.stdout;
		Ok(State {
			cacheable,
			checksum,
			children,
			command,
			created_at,
			dequeued_at,
			enqueued_at,
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
		})
	}
}
