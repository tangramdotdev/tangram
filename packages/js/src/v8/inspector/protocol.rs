use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub(super) struct BaseMessage {
	pub(super) id: Option<i32>,
	pub(super) method: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct CdpCommandEnvelope {
	pub(super) id: Option<i64>,
	pub(super) method: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ScriptParsedNotification {
	pub(super) params: ScriptParsedParams,
}

#[derive(Debug, Serialize)]
pub(super) struct CdpNotification {
	pub(super) method: &'static str,
}

#[derive(Debug, Serialize)]
pub(super) struct CdpResponse<T> {
	pub(super) id: i64,
	pub(super) result: T,
}

#[derive(Debug, Serialize)]
pub(super) struct EmptyResult {}

#[derive(Debug, Serialize)]
pub(super) struct Request<P> {
	pub(super) id: i32,
	pub(super) method: &'static str,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(super) params: Option<P>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvaluateParams<'a> {
	pub(super) expression: &'a str,
	pub(super) await_promise: bool,
	pub(super) repl_mode: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(super) context_id: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CallFunctionOnParams<'a> {
	pub(super) function_declaration: &'a str,
	pub(super) arguments: Vec<CallArgument<'a>>,
	pub(super) await_promise: bool,
	pub(super) return_by_value: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(super) execution_context_id: Option<u64>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CallArgument<'a> {
	#[serde(skip_serializing_if = "Option::is_none")]
	object_id: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	unserializable_value: Option<&'a str>,
	#[serde(skip_serializing_if = "Option::is_none")]
	value: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Response {
	pub(super) result: Option<EvaluateResponse>,
	pub(super) error: Option<ProtocolError>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ProtocolError {
	pub(super) code: i64,
	pub(super) message: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvaluateResponse {
	pub(super) result: Option<RemoteObject>,
	pub(super) exception_details: Option<ExceptionDetails>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExceptionDetails {
	pub(super) text: Option<String>,
	pub(super) exception: Option<RemoteObject>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RemoteObject {
	#[serde(rename = "type")]
	pub(super) kind: Option<String>,
	pub(super) subtype: Option<String>,
	pub(super) value: Option<serde_json::Value>,
	pub(super) unserializable_value: Option<String>,
	pub(super) description: Option<String>,
	pub(super) object_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExecutionContextCreatedNotification {
	pub(super) params: ExecutionContextCreatedParams,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExecutionContextCreatedParams {
	pub(super) context: ExecutionContextDescription,
}

#[derive(Debug, Deserialize)]
pub(super) struct ExecutionContextDescription {
	pub(super) id: u64,
}

#[derive(Debug, Deserialize)]
pub(super) struct ScriptParsedParams {
	pub(super) url: String,
}

pub(super) enum ResponseResult<'a> {
	Value(&'a RemoteObject),
	Exception(&'a ExceptionDetails),
}

impl<'a> CallArgument<'a> {
	pub(super) fn new(remote: &'a RemoteObject) -> Self {
		if let Some(object_id) = remote.object_id.as_deref() {
			Self {
				object_id: Some(object_id),
				unserializable_value: None,
				value: None,
			}
		} else if let Some(unserializable_value) = remote.unserializable_value.as_deref() {
			Self {
				object_id: None,
				unserializable_value: Some(unserializable_value),
				value: None,
			}
		} else {
			Self {
				object_id: None,
				unserializable_value: None,
				value: if remote.subtype.as_deref() == Some("null") {
					Some(serde_json::Value::Null)
				} else {
					remote.value.clone()
				},
			}
		}
	}
}

pub(super) fn response_result(response: &Response) -> Result<ResponseResult<'_>, String> {
	if let Some(error) = &response.error {
		return Err(format!("{}: {}", error.code, error.message));
	}

	let result = response
		.result
		.as_ref()
		.ok_or_else(|| "malformed evaluation response".to_owned())?;

	if let Some(details) = &result.exception_details {
		return Ok(ResponseResult::Exception(details));
	}

	result
		.result
		.as_ref()
		.map(ResponseResult::Value)
		.ok_or_else(|| "malformed evaluation result".to_owned())
}
