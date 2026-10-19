use {
	super::{Project, convert::script_element_kind, position, range},
	crate::call_hierarchy,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn call_hierarchy_prepare(
		&self,
		arg: &call_hierarchy::PrepareArg,
	) -> tg::Result<call_hierarchy::PrepareOutput> {
		self.with_language_service(&arg.module, |context, service| {
			let output = service
				.provide_prepare_call_hierarchy(
					context,
					&self.uri(&arg.module),
					position(arg.position),
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
			let output = call_hierarchy::PrepareOutput { items };

			Ok(output)
		})
	}

	pub(super) fn call_hierarchy_incoming(
		&self,
		arg: &call_hierarchy::IncomingArg,
	) -> tg::Result<call_hierarchy::IncomingOutput> {
		self.with_language_service(&arg.module, |context, service| {
			let prepared = service
				.provide_prepare_call_hierarchy(
					context,
					&self.uri(&arg.module),
					position(arg.position),
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
			let output = call_hierarchy::IncomingOutput { calls };

			Ok(output)
		})
	}

	pub(super) fn call_hierarchy_outgoing(
		&self,
		arg: &call_hierarchy::OutgoingArg,
	) -> tg::Result<call_hierarchy::OutgoingOutput> {
		self.with_language_service(&arg.module, |context, service| {
			let prepared = service
				.provide_prepare_call_hierarchy(
					context,
					&self.uri(&arg.module),
					position(arg.position),
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
			let output = call_hierarchy::OutgoingOutput { calls };

			Ok(output)
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
