use {
	super::{Project, position},
	crate::signature_help,
	tangram_client::prelude::*,
	tsrs_lsproto as native,
};

impl Project {
	pub(super) fn signature_help(
		&self,
		request: &signature_help::Request,
	) -> tg::Result<signature_help::Response> {
		self.with_language_service(&request.module, |context, service| {
			let uri = self.uri(&request.module);
			let output = service
				.provide_signature_help(context, &uri, position(request.position), None)
				.map_err(|error| tg::error!(!error, "failed to get tsrs signature help"))?;
			let help = output
				.signature_help
				.map(|help| {
					let active_parameter = help.active_parameter.and_then(|value| value.uinteger);
					let active_signature = help.active_signature;
					let signatures = help
						.signatures
						.into_iter()
						.map(|signature| {
							let label = signature.label;
							let parameters = signature
								.parameters
								.unwrap_or_default()
								.into_iter()
								.map(|parameter| {
									let label = parameter_label(&label, parameter.label)?;
									let documentation =
										parameter.documentation.map(|value| value.as_string());
									Ok(signature_help::Parameter {
										documentation,
										label,
									})
								})
								.collect::<tg::Result<Vec<_>>>()?;
							let active_parameter =
								signature.active_parameter.and_then(|value| value.uinteger);
							let documentation =
								signature.documentation.map(|value| value.as_string());
							let signature = signature_help::Signature {
								active_parameter,
								documentation,
								label,
								parameters,
							};
							Ok(signature)
						})
						.collect::<tg::Result<Vec<_>>>()?;
					let help = signature_help::Help {
						active_parameter,
						active_signature,
						signatures,
					};
					Ok::<_, tg::Error>(help)
				})
				.transpose()?;
			let response = signature_help::Response { help };

			Ok(response)
		})
	}
}

fn parameter_label(signature: &str, label: native::StringOrTuple) -> tg::Result<String> {
	if let Some(label) = label.string {
		return Ok(label);
	}
	let Some([start, end]) = label.tuple else {
		return Ok(String::new());
	};
	let signature = signature.encode_utf16().collect::<Vec<_>>();
	let label = signature
		.get(start as usize..end as usize)
		.ok_or_else(|| tg::error!("invalid tsrs signature parameter range"))?;
	let label = String::from_utf16(label)
		.map_err(|error| tg::error!(!error, "invalid tsrs signature parameter label"))?;

	Ok(label)
}
