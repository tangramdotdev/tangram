use crate::prelude::*;

#[derive(Clone, Debug)]
pub struct State {
	pub actual_checksum: Option<tg::Checksum>,
	pub cacheable: bool,
	pub children: Option<Vec<Child>>,
	pub command: tg::Command,
	pub created_at: i64,
	pub error: Option<tg::Error>,
	pub exit: Option<u8>,
	pub expected_checksum: Option<tg::Checksum>,
	pub finished_at: Option<i64>,
	pub log: Option<tg::Blob>,
	pub output: Option<tg::Value>,
	pub retry: bool,
	pub sandbox: Option<tg::sandbox::Id>,
	pub started_at: Option<i64>,
	pub status: tg::process::Status,
	pub stderr: tg::process::Stdio,
	pub stdin: tg::process::Stdio,
	pub stdout: tg::process::Stdio,
	pub tty: Option<tg::process::Tty>,
}

#[derive(Clone, Debug)]
pub struct Child {
	pub process: tg::Process,
	pub options: tg::referent::Options,
}

impl TryFrom<tg::process::Data> for tg::process::State {
	type Error = tg::Error;

	fn try_from(value: tg::process::Data) -> Result<Self, Self::Error> {
		let actual_checksum = value.actual_checksum;
		let cacheable = value.cacheable;
		let children = value
			.children
			.map(|children| children.into_iter().map(Into::into).collect());
		let command = tg::Command::with_id(value.command);
		let created_at = value.created_at;
		let error = value
			.error
			.map(|either| match either {
				tg::Either::Left(data) => {
					let object = tg::error::Object::try_from_data(data)?;
					Ok::<_, tg::Error>(tg::Error::with_object(object))
				},
				tg::Either::Right(id) => Ok(tg::Error::with_id(id)),
			})
			.transpose()?;
		let exit = value.exit;
		let expected_checksum = value.expected_checksum;
		let finished_at = value.finished_at;
		let log = value.log.map(tg::Blob::with_id);
		let output = value.output.map(tg::Value::try_from).transpose()?;
		let retry = value.retry;
		let sandbox = value.sandbox;
		let started_at = value.started_at;
		let status = value.status;
		let stderr = value.stderr;
		let stdin = value.stdin;
		let stdout = value.stdout;
		let tty = value.tty;
		Ok(State {
			actual_checksum,
			cacheable,
			children,
			command,
			created_at,
			error,
			exit,
			expected_checksum,
			finished_at,
			log,
			output,
			retry,
			sandbox,
			started_at,
			status,
			stderr,
			stdin,
			stdout,
			tty,
		})
	}
}

impl From<tg::process::data::Child> for Child {
	fn from(value: tg::process::data::Child) -> Self {
		Self {
			process: tg::Process::new_with_cached(
				value.process,
				None,
				None,
				None,
				None,
				Some(value.cached),
			),
			options: value.options,
		}
	}
}
