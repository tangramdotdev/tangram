use serde_json::Value;

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InitializeResponse {
	pub current_directory: String,
	pub use_case_sensitive_file_names: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateSnapshotParams {
	pub create_programs: Vec<CreateProgramParams>,
	pub ensure_programs: bool,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProgramParams {
	pub compiler_options: Value,
	pub options: CreateProgramOptions,
	pub root_files: Vec<String>,
}

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateProgramOptions {
	pub module_resolver: u64,
}

#[derive(Debug, serde::Deserialize)]
pub struct CreateSnapshotResponse {
	pub operation: SnapshotOperation,
	pub snapshot: u64,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotOperation {
	#[serde(default)]
	pub created_programs: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResolveModuleNameParams {
	pub containing_directory: String,
	pub module_name: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Diagnostic {
	pub category: u32,
	pub end_position: Option<Position>,
	pub file_name: Option<String>,
	#[serde(default)]
	pub message_chain: Vec<Diagnostic>,
	pub start_position: Option<Position>,
	pub text: String,
}

#[derive(Clone, Copy, Debug, serde::Deserialize)]
pub struct Position {
	pub character: u32,
	pub line: u32,
}
