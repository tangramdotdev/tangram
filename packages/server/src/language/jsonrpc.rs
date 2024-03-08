pub const VERSION: &str = "2.0";

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Message {
	Request(Request),
	Response(Response),
	Notification(Notification),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Id {
	String(String),
	I32(i32),
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Request {
	pub jsonrpc: String,
	pub id: Id,
	pub method: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub params: Option<serde_json::Value>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Response {
	pub jsonrpc: String,
	pub id: Id,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub result: Option<serde_json::Value>,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub error: Option<ResponseError>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ResponseError {
	pub code: ResponseErrorCode,
	pub message: String,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum ResponseErrorCode {
	// The following error codes are defined by JSON-RPC.
	ParseError = -32700,
	InvalidRequest = -32600,
	MethodNotFound = -32601,
	InvalidParams = -32602,
	InternalError = -32603,

	// The following error codes are defined by the language server protocol.
	ServerNotInitialized = -32002,
	UnknownErrorCode = -32001,
	RequestFailed = -32803,
	ServerCancelled = -32802,
	ContentModified = -32801,
	RequestCancelled = -32800,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Notification {
	pub jsonrpc: String,
	pub method: String,
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub params: Option<serde_json::Value>,
}
