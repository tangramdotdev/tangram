use {crate::prelude::*, std::collections::BTreeMap};

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Import {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub kind: Option<tg::module::Kind>,

	pub reference: tg::Reference,
}

impl Import {
	pub fn with_specifier_and_attributes(
		specifier: &str,
		mut attributes: Option<BTreeMap<String, String>>,
	) -> tg::Result<Self> {
		// Parse the specifier as a reference.
		let reference = specifier.parse::<tg::Reference>()?;

		let kind = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("type").or(attributes.remove("kind")))
			.map(|kind| kind.parse())
			.transpose()?;
		let source = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("source"))
			.map(Into::into);
		let locations = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("location"))
			.map(|locations| {
				serde_qs::from_str::<tg::location::Locations>(&locations)
					.map_err(|source| tg::error!(!source, "invalid location attribute"))
			})
			.transpose()?;
		let path = attributes
			.as_mut()
			.and_then(|attributes| attributes.remove("path"))
			.map(Into::into);

		let options = tg::reference::Options {
			locations: tg::location::Locations {
				local: locations
					.as_ref()
					.and_then(|locations| locations.local.clone())
					.or(reference.options().locations.local.clone()),
				remotes: locations
					.as_ref()
					.and_then(|locations| locations.remotes.clone())
					.or(reference.options().locations.remotes.clone()),
			},
			path: path.or(reference.options().path.clone()),
			source: source.or(reference.options().source.clone()),
		};
		let reference = tg::Reference::with_item_and_options(reference.item().clone(), options);

		let import = Import { kind, reference };

		Ok(import)
	}
}
