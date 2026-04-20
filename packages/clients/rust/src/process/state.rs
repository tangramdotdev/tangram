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
	pub host: String,
	pub log: Option<tg::Blob>,
	pub output: Option<tg::Value>,
	pub retry: bool,
	pub sandbox: tg::sandbox::Id,
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

impl State {
	#[must_use]
	pub fn to_data(&self) -> tg::process::Data {
		let actual_checksum = self.actual_checksum.clone();
		let cacheable = self.cacheable;
		let children = self
			.children
			.as_ref()
			.map(|children| children.iter().map(Child::to_data).collect());
		let command = self.command.id().clone();
		let created_at = self.created_at;
		let error = self.error.as_ref().map(tg::Error::to_data_or_id);
		let exit = self.exit;
		let expected_checksum = self.expected_checksum.clone();
		let finished_at = self.finished_at;
		let host = self.host.clone();
		let log = self.log.as_ref().map(tg::Blob::id);
		let sandbox = self.sandbox.clone();
		let output = self.output.as_ref().map(tg::Value::to_data);
		let retry = self.retry;
		let started_at = self.started_at;
		let status = self.status;
		let stderr = self.stderr.clone();
		let stdin = self.stdin.clone();
		let stdout = self.stdout.clone();
		let tty = self.tty;
		tg::process::Data {
			actual_checksum,
			cacheable,
			children,
			command,
			created_at,
			error,
			exit,
			expected_checksum,
			finished_at,
			host,
			log,
			sandbox,
			output,
			retry,
			started_at,
			status,
			stderr,
			stdin,
			stdout,
			tty,
		}
	}

	pub fn try_from_data(value: tg::process::Data) -> tg::Result<Self> {
		let actual_checksum = value.actual_checksum;
		let cacheable = value.cacheable;
		let children = value
			.children
			.map(|children| {
				children
					.into_iter()
					.map(Child::try_from_data)
					.collect::<tg::Result<Vec<_>>>()
			})
			.transpose()?;
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
		let host = value.host;
		let log = value.log.map(tg::Blob::with_id);
		let output = value.output.map(tg::Value::try_from_data).transpose()?;
		let retry = value.retry;
		let sandbox = value.sandbox;
		let started_at = value.started_at;
		let status = value.status;
		let stderr = value.stderr;
		let stdin = value.stdin;
		let stdout = value.stdout;
		let tty = value.tty;
		Ok(Self {
			actual_checksum,
			cacheable,
			children,
			command,
			created_at,
			error,
			exit,
			expected_checksum,
			finished_at,
			host,
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

impl Child {
	#[must_use]
	pub fn to_data(&self) -> tg::process::data::Child {
		tg::process::data::Child {
			cached: self.process.cached().unwrap_or(false),
			options: self.options.clone(),
			process: self.process.id().clone(),
		}
	}

	pub fn try_from_data(value: tg::process::data::Child) -> tg::Result<Self> {
		Ok(Self {
			process: tg::Process::new(value.process, None, None, None, None, Some(value.cached)),
			options: value.options,
		})
	}
}

impl TryFrom<tg::process::Data> for tg::process::State {
	type Error = tg::Error;

	fn try_from(value: tg::process::Data) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}

impl TryFrom<tg::process::data::Child> for Child {
	type Error = tg::Error;

	fn try_from(value: tg::process::data::Child) -> Result<Self, Self::Error> {
		Self::try_from_data(value)
	}
}
