use {
	super::Compiler,
	futures::{TryStreamExt as _, stream::FuturesOrdered},
	lsp_types as lsp,
	tangram_client::prelude::*,
};

#[derive(Debug, serde::Serialize)]
pub struct PrepareRequest {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct PrepareResponse {
	pub items: Option<Vec<Item>>,
}

#[derive(Debug, serde::Serialize)]
pub struct IncomingRequest {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct IncomingResponse {
	pub calls: Option<Vec<IncomingCall>>,
}

#[derive(Debug, serde::Serialize)]
pub struct OutgoingRequest {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
pub struct OutgoingResponse {
	pub calls: Option<Vec<OutgoingCall>>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Item {
	pub name: String,
	pub kind: String,
	pub detail: Option<String>,
	pub module: tg::module::Data,
	pub range: tg::Range,
	pub selection: tg::Range,
	pub container_name: Option<String>,
	pub data: Option<ItemData>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ItemData {
	pub module: tg::module::Data,
	pub position: tg::Position,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IncomingCall {
	pub from: Item,
	pub from_ranges: Vec<tg::Range>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OutgoingCall {
	pub to: Item,
	pub from_ranges: Vec<tg::Range>,
}

impl Compiler {
	pub async fn call_hierarchy_prepare(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<Item>>> {
		// Create the request.
		let request = super::Request::CallHierarchyPrepare(PrepareRequest {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::CallHierarchyPrepare(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.items)
	}

	pub async fn call_hierarchy_incoming(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<IncomingCall>>> {
		// Create the request.
		let request = super::Request::CallHierarchyIncoming(IncomingRequest {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::CallHierarchyIncoming(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.calls)
	}

	pub async fn call_hierarchy_outgoing(
		&self,
		module: &tg::module::Data,
		position: tg::Position,
	) -> tg::Result<Option<Vec<OutgoingCall>>> {
		// Create the request.
		let request = super::Request::CallHierarchyOutgoing(OutgoingRequest {
			module: module.clone(),
			position,
		});

		// Perform the request.
		let response = self.request(request).await?;

		// Get the response.
		let super::Response::CallHierarchyOutgoing(response) = response else {
			return Err(tg::error!("unexpected response type"));
		};

		Ok(response.calls)
	}
}

impl Compiler {
	pub(crate) async fn handle_call_hierarchy_prepare_request(
		&self,
		params: lsp::CallHierarchyPrepareParams,
	) -> tg::Result<Option<Vec<lsp::CallHierarchyItem>>> {
		// Get the module.
		let module = self
			.module_for_lsp_uri(&params.text_document_position_params.text_document.uri)
			.await?;

		// Get the position for the request.
		let position = params.text_document_position_params.position;

		// Get the call hierarchy items.
		let items = self
			.call_hierarchy_prepare(&module, position.into())
			.await?;
		let Some(items) = items else {
			return Ok(None);
		};

		// Convert the items.
		let items = items
			.into_iter()
			.map(|item| {
				let compiler = self.clone();
				async move { compiler.call_hierarchy_item_to_lsp(item).await }
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		Ok(Some(items))
	}

	pub(crate) async fn handle_call_hierarchy_incoming_calls_request(
		&self,
		params: lsp::CallHierarchyIncomingCallsParams,
	) -> tg::Result<Option<Vec<lsp::CallHierarchyIncomingCall>>> {
		// Determine the module and position.
		let (module, position) = self
			.call_hierarchy_item_data_to_module_and_position(params.item)
			.await?;

		// Get incoming calls.
		let calls = self.call_hierarchy_incoming(&module, position).await?;
		let Some(calls) = calls else {
			return Ok(None);
		};

		// Convert the calls.
		let calls = calls
			.into_iter()
			.map(|call| {
				let compiler = self.clone();
				async move {
					let from = compiler.call_hierarchy_item_to_lsp(call.from).await?;
					let from_ranges = call.from_ranges.into_iter().map(Into::into).collect();
					let call = lsp::CallHierarchyIncomingCall { from, from_ranges };
					Ok::<_, tg::Error>(call)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		Ok(Some(calls))
	}

	pub(crate) async fn handle_call_hierarchy_outgoing_calls_request(
		&self,
		params: lsp::CallHierarchyOutgoingCallsParams,
	) -> tg::Result<Option<Vec<lsp::CallHierarchyOutgoingCall>>> {
		// Determine the module and position.
		let (module, position) = self
			.call_hierarchy_item_data_to_module_and_position(params.item)
			.await?;

		// Get outgoing calls.
		let calls = self.call_hierarchy_outgoing(&module, position).await?;
		let Some(calls) = calls else {
			return Ok(None);
		};

		// Convert the calls.
		let calls = calls
			.into_iter()
			.map(|call| {
				let compiler = self.clone();
				async move {
					let to = compiler.call_hierarchy_item_to_lsp(call.to).await?;
					let from_ranges = call.from_ranges.into_iter().map(Into::into).collect();
					let call = lsp::CallHierarchyOutgoingCall { to, from_ranges };
					Ok::<_, tg::Error>(call)
				}
			})
			.collect::<FuturesOrdered<_>>()
			.try_collect()
			.await?;

		Ok(Some(calls))
	}

	async fn call_hierarchy_item_data_to_module_and_position(
		&self,
		item: lsp::CallHierarchyItem,
	) -> tg::Result<(tg::module::Data, tg::Position)> {
		if let Some(data) = item.data
			&& let Ok(data) = serde_json::from_value::<ItemData>(data)
		{
			return Ok((data.module, data.position));
		}

		let module = self.module_for_lsp_uri(&item.uri).await?;
		let position = item.selection_range.start.into();
		Ok((module, position))
	}

	async fn call_hierarchy_item_to_lsp(&self, item: Item) -> tg::Result<lsp::CallHierarchyItem> {
		let Item {
			name,
			kind,
			detail,
			module,
			range,
			selection,
			container_name,
			data,
		} = item;
		let uri = self.lsp_uri_for_module(&module).await?;
		let kind = symbol_kind_for_script_element_kind(&kind);
		let data = data.map(|data| serde_json::to_value(data).unwrap());
		let item = lsp::CallHierarchyItem {
			name,
			kind,
			tags: None,
			detail: detail.or(container_name),
			uri,
			range: range.into(),
			selection_range: selection.into(),
			data,
		};
		Ok(item)
	}
}

fn symbol_kind_for_script_element_kind(kind: &str) -> lsp::SymbolKind {
	match kind {
		"class" | "local class" => lsp::SymbolKind::CLASS,
		"enum" => lsp::SymbolKind::ENUM,
		"enum member" => lsp::SymbolKind::ENUM_MEMBER,
		"interface" => lsp::SymbolKind::INTERFACE,
		"module" | "external module name" => lsp::SymbolKind::MODULE,
		"type" | "primitive type" | "type parameter" => lsp::SymbolKind::TYPE_PARAMETER,
		"const" => lsp::SymbolKind::CONSTANT,
		"property" | "accessor" | "getter" | "setter" => lsp::SymbolKind::PROPERTY,
		"method" => lsp::SymbolKind::METHOD,
		"function" | "local function" => lsp::SymbolKind::FUNCTION,
		"constructor" | "construct" => lsp::SymbolKind::CONSTRUCTOR,
		"directory" | "script" => lsp::SymbolKind::FILE,
		_ => lsp::SymbolKind::VARIABLE,
	}
}
