use {
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

/// Helper struct for deserializing process metadata from SQLite rows.
#[derive(db::sqlite::row::Deserialize)]
pub(super) struct Row {
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_command_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_command_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_command_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_error_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_error_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_error_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_log_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_log_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_log_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_output_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_output_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub node_output_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_command_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_command_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_command_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_error_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_error_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_error_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_log_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_log_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_log_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_output_count: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_output_depth: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_output_size: Option<u64>,
	#[tangram_database(as = "Option<db::sqlite::value::TryFrom<i64>>")]
	pub subtree_count: Option<u64>,
}

impl Row {
	pub fn to_metadata(&self) -> tg::process::Metadata {
		let node = tg::process::metadata::Node {
			command: tg::object::metadata::Subtree {
				count: self.node_command_count,
				depth: self.node_command_depth,
				size: self.node_command_size,
				solvable: None,
				solved: None,
			},
			error: tg::object::metadata::Subtree {
				count: self.node_error_count,
				depth: self.node_error_depth,
				size: self.node_error_size,
				solvable: None,
				solved: None,
			},
			log: tg::object::metadata::Subtree {
				count: self.node_log_count,
				depth: self.node_log_depth,
				size: self.node_log_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: self.node_output_count,
				depth: self.node_output_depth,
				size: self.node_output_size,
				solvable: None,
				solved: None,
			},
		};
		let subtree = tg::process::metadata::Subtree {
			command: tg::object::metadata::Subtree {
				count: self.subtree_command_count,
				depth: self.subtree_command_depth,
				size: self.subtree_command_size,
				solvable: None,
				solved: None,
			},
			error: tg::object::metadata::Subtree {
				count: self.subtree_error_count,
				depth: self.subtree_error_depth,
				size: self.subtree_error_size,
				solvable: None,
				solved: None,
			},
			log: tg::object::metadata::Subtree {
				count: self.subtree_log_count,
				depth: self.subtree_log_depth,
				size: self.subtree_log_size,
				solvable: None,
				solved: None,
			},
			output: tg::object::metadata::Subtree {
				count: self.subtree_output_count,
				depth: self.subtree_output_depth,
				size: self.subtree_output_size,
				solvable: None,
				solved: None,
			},
			count: self.subtree_count,
		};
		tg::process::Metadata { node, subtree }
	}
}
