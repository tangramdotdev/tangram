use {
	serde::{Deserialize, Serialize},
	uuid::Uuid,
};

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TargetMetadata {
	pub description: String,
	pub devtools_frontend_url: String,
	pub id: Uuid,
	pub title: String,
	#[serde(rename = "type")]
	pub target_type: String,
	pub url: String,
	pub web_socket_debugger_url: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct VersionMetadata {
	#[serde(rename = "Browser")]
	pub browser: String,
	#[serde(rename = "Protocol-Version")]
	pub protocol_version: String,
	#[serde(rename = "V8-Version")]
	pub v8_version: String,
}

#[derive(Debug, Deserialize)]
pub struct CdpCommandEnvelope {
	pub id: Option<i64>,
	pub method: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct CdpResponse<T> {
	pub id: i64,
	pub result: T,
}

#[derive(Debug, Serialize)]
pub struct EmptyResult {}

#[derive(Debug, Serialize)]
pub struct CdpNotification {
	pub method: &'static str,
}

#[derive(Debug, Deserialize)]
pub struct BaseMessage {
	pub id: Option<i32>,
	pub method: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct InspectorRequest<P> {
	pub id: i32,
	pub method: &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub params: Option<P>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEvaluateParams<'a> {
	pub expression: &'a str,
	pub await_promise: bool,
	pub repl_mode: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub context_id: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct InspectorResponse {
	pub result: Option<RuntimeEvaluateResponse>,
	pub error: Option<ProtocolError>,
}

#[derive(Debug, Deserialize)]
pub struct ProtocolError {
	pub code: i64,
	pub message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeEvaluateResponse {
	pub result: Option<RemoteObject>,
	pub exception_details: Option<ExceptionDetails>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExceptionDetails {
	pub text: Option<String>,
	pub exception: Option<RemoteObject>,
}

#[derive(Debug, Deserialize)]
pub struct RemoteObject {
	#[serde(rename = "type")]
	pub kind: Option<String>,
	pub subtype: Option<String>,
	pub value: Option<RemoteValue>,
	pub unserializable_value: Option<String>,
	pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum RemoteValue {
	Bool(bool),
	Number(f64),
	String(String),
}

#[derive(Debug, Deserialize)]
pub struct ExecutionContextCreatedNotification {
	pub params: ExecutionContextCreatedParams,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionContextCreatedParams {
	pub context: ExecutionContextDescription,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionContextDescription {
	pub id: u64,
}
