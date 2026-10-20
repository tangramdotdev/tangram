use {
	crate::prelude::*,
	serde_with::{DisplayFromStr, serde_as},
	tangram_util::serde::{is_default, is_false},
};

mod command;
#[cfg(test)]
mod tests;

pub use self::command::Command;

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Data {
	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_false")]
	pub cacheable: bool,

	#[serde(default, skip_serializing_if = "tg::process::Checksum::is_empty")]
	#[tangram_serialize(
		default,
		id = 1,
		skip_serializing_if = "tg::process::Checksum::is_empty"
	)]
	pub checksum: tg::process::Checksum,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub children: Option<Vec<tg::process::data::Child>>,

	#[serde_as(as = "tg::referent::Either")]
	#[tangram_serialize(id = 3)]
	pub command: tg::Referent<tg::Either<Box<Command>, tg::command::Id>>,

	#[tangram_serialize(id = 4)]
	pub created_at: i64,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub debug: Option<tg::process::Debug>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<tg::referent::Either>")]
	#[tangram_serialize(default, id = 6, skip_serializing_if = "Option::is_none")]
	pub error: Option<tg::Referent<tg::Either<Box<tg::error::Data>, tg::error::Id>>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 7, skip_serializing_if = "Option::is_none")]
	pub exit: Option<u8>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 8, skip_serializing_if = "Option::is_none")]
	pub finished_at: Option<i64>,

	#[tangram_serialize(id = 9)]
	pub host: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DisplayFromStr>")]
	#[tangram_serialize(default, id = 10, skip_serializing_if = "Option::is_none")]
	pub log: Option<tg::Referent<tg::blob::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 11, skip_serializing_if = "Option::is_none")]
	pub output: Option<tg::Referent<tg::value::Data>>,

	#[serde(default, skip_serializing_if = "is_false")]
	#[tangram_serialize(default, id = 12, skip_serializing_if = "is_false")]
	pub retry: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 13, skip_serializing_if = "Option::is_none")]
	pub sandbox: Option<tg::sandbox::Id>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 14, skip_serializing_if = "Option::is_none")]
	pub started_at: Option<i64>,

	#[tangram_serialize(id = 15)]
	pub status: tg::process::Status,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 16, skip_serializing_if = "is_default")]
	pub stderr: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 17, skip_serializing_if = "is_default")]
	pub stdin: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "is_default")]
	#[tangram_serialize(default, id = 18, skip_serializing_if = "is_default")]
	pub stdout: tg::process::Stdio,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 19, skip_serializing_if = "Option::is_none")]
	pub tty: Option<tg::process::Tty>,
}

#[serde_as]
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Child {
	#[tangram_serialize(id = 1)]
	#[serde(default, skip_serializing_if = "is_false")]
	pub cached: bool,

	#[tangram_serialize(id = 2)]
	#[serde_as(as = "DisplayFromStr")]
	pub process: tg::Referent<tg::process::Id>,
}

impl Data {
	#[must_use]
	pub fn without_location_and_tokens(mut self) -> Self {
		self.command.options.clear_location_and_tokens();
		self.command.node = self
			.command
			.node
			.map_left(|command| Box::new(command.without_location_and_tokens()));
		self.children = self.children.map(|children| {
			children
				.into_iter()
				.map(Child::without_location_and_tokens)
				.collect()
		});
		self.error = self.error.map(|mut error| {
			error.options.clear_location_and_tokens();
			error.node = error
				.node
				.map_left(|data| Box::new((*data).without_location_and_tokens()));
			error
		});
		self.log = self.log.map(|mut log| {
			log.options.clear_location_and_tokens();
			log
		});
		self.output = self.output.map(|mut output| {
			output.options.clear_location_and_tokens();
			output.node = output.node.without_location_and_tokens();
			output
		});

		self
	}

	/// Remove the locations and tokens, except the local sync tokens on the command, error, log, and output referents.
	#[must_use]
	pub fn without_location_and_tokens_except_sync(mut self) -> Self {
		// Take the outer referent options before removing the tokens.
		let mut command = std::mem::take(&mut self.command.options);
		let error = self
			.error
			.as_mut()
			.map(|error| std::mem::take(&mut error.options));
		let log = self
			.log
			.as_mut()
			.map(|log| std::mem::take(&mut log.options));
		let output = self
			.output
			.as_mut()
			.map(|output| std::mem::take(&mut output.options));

		// Remove the locations and tokens.
		let mut data = self.without_location_and_tokens();

		// Restore the outer referent options with only their local sync tokens.
		command.clear_location_and_tokens_except_sync();
		data.command.options = command;
		if let (Some(mut options), Some(error)) = (error, &mut data.error) {
			options.clear_location_and_tokens_except_sync();
			error.options = options;
		}
		if let (Some(mut options), Some(log)) = (log, &mut data.log) {
			options.clear_location_and_tokens_except_sync();
			log.options = options;
		}
		if let (Some(mut options), Some(output)) = (output, &mut data.output) {
			options.clear_location_and_tokens_except_sync();
			output.options = options;
		}

		data
	}

	/// Set the error, moving the location and tokens of an error ID's referent to the error referent.
	pub fn set_error(
		&mut self,
		error: Option<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>>,
	) {
		self.error = error.map(|error| match error {
			tg::Either::Left(data) => tg::Referent::with_node(tg::Either::Left(Box::new(data))),
			tg::Either::Right(referent) => referent.map(tg::Either::Right),
		});
	}

	/// Set the output, leaving the tokens on the output's object referents.
	pub fn set_output(&mut self, output: Option<tg::value::Data>) {
		self.output = output.map(tg::Referent::with_node);
	}

	/// Get the error, with the error referent's location and tokens inherited by an error ID.
	#[must_use]
	pub fn error_with_location_and_tokens(
		&self,
	) -> Option<tg::Either<tg::error::Data, tg::Referent<tg::error::Id>>> {
		let error = self.error.as_ref()?;
		let error = match &error.node {
			tg::Either::Left(data) => tg::Either::Left(data.as_ref().clone()),
			tg::Either::Right(id) => {
				tg::Either::Right(tg::Referent::new(id.clone(), error.options.clone()))
			},
		};
		Some(error)
	}

	/// Get the output, with the output referent's location and tokens inherited by the output's object referents.
	#[must_use]
	pub fn output_with_location_and_tokens(&self) -> Option<tg::value::Data> {
		let output = self.output.as_ref()?;
		let mut value = output.node.clone();
		command::inherit_value_location_and_tokens(&mut value, &output.options);
		Some(value)
	}
}

impl Child {
	#[must_use]
	pub fn without_location_and_tokens(mut self) -> Self {
		self.process.options.clear_location_and_tokens();

		self
	}
}
