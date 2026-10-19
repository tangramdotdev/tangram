use {
	super::{Project, convert::script_element_kind, position, range},
	crate::call_hierarchy,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn call_hierarchy_prepare(
		&self,
		request: &call_hierarchy::PrepareRequest,
	) -> tg::Result<call_hierarchy::PrepareResponse> {
		self.with_language_service(&request.module, |context, service| {
			let output = service
				.provide_prepare_call_hierarchy(
					context,
					&self.uri(&request.module),
					position(request.position),
				)
				.map_err(|error| tg::error!(!error, "failed to prepare the tsrs call hierarchy"))?;
			let items = output
				.call_hierarchy_items
				.map(|items| {
					items
						.into_iter()
						.map(|item| self.hierarchy_item(item))
						.collect()
				})
				.transpose()?;
			let response = call_hierarchy::PrepareResponse { items };

			Ok(response)
		})
	}

	pub(super) fn call_hierarchy_incoming(
		&self,
		request: &call_hierarchy::IncomingRequest,
	) -> tg::Result<call_hierarchy::IncomingResponse> {
		self.with_language_service(&request.module, |context, service| {
			let prepared = service
				.provide_prepare_call_hierarchy(
					context,
					&self.uri(&request.module),
					position(request.position),
				)
				.map_err(|error| tg::error!(!error, "failed to prepare the tsrs call hierarchy"))?;
			let mut calls = Vec::new();
			for item in prepared.call_hierarchy_items.unwrap_or_default() {
				let output = service
					.provide_call_hierarchy_incoming_calls(context, &item, None)
					.map_err(|error| tg::error!(!error, "failed to get the incoming tsrs calls"))?;
				for call in output.call_hierarchy_incoming_calls.unwrap_or_default() {
					let from = self.hierarchy_item(call.from)?;
					let from_ranges = call.from_ranges.into_iter().map(range).collect();
					let call = call_hierarchy::IncomingCall { from, from_ranges };
					calls.push(call);
				}
			}
			let calls = (!calls.is_empty()).then_some(calls);
			let response = call_hierarchy::IncomingResponse { calls };

			Ok(response)
		})
	}

	pub(super) fn call_hierarchy_outgoing(
		&self,
		request: &call_hierarchy::OutgoingRequest,
	) -> tg::Result<call_hierarchy::OutgoingResponse> {
		self.with_language_service(&request.module, |context, service| {
			let prepared = service
				.provide_prepare_call_hierarchy(
					context,
					&self.uri(&request.module),
					position(request.position),
				)
				.map_err(|error| tg::error!(!error, "failed to prepare the tsrs call hierarchy"))?;
			let mut calls = Vec::new();
			for item in prepared.call_hierarchy_items.unwrap_or_default() {
				let output = service
					.provide_call_hierarchy_outgoing_calls(context, &item)
					.map_err(|error| tg::error!(!error, "failed to get the outgoing tsrs calls"))?;
				for call in output.call_hierarchy_outgoing_calls.unwrap_or_default() {
					let from_ranges = call.from_ranges.into_iter().map(range).collect();
					let to = self.hierarchy_item(call.to)?;
					let call = call_hierarchy::OutgoingCall { from_ranges, to };
					calls.push(call);
				}
			}
			let calls = (!calls.is_empty()).then_some(calls);
			let response = call_hierarchy::OutgoingResponse { calls };

			Ok(response)
		})
	}

	fn hierarchy_item(&self, item: native::CallHierarchyItem) -> tg::Result<call_hierarchy::Item> {
		let location = native::Location {
			range: item.range,
			uri: item.uri,
		};
		let location = self.location(&location)?;
		let kind = script_element_kind(item.kind)?.to_owned();
		let selection = range(item.selection_range);
		let data = Some(call_hierarchy::ItemData {
			module: location.module.clone(),
			position: selection.start,
		});
		let item = call_hierarchy::Item {
			container_name: None,
			data,
			detail: item.detail,
			kind,
			module: location.module,
			name: item.name,
			range: location.range,
			selection,
		};

		Ok(item)
	}
}
