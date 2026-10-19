use {
	super::{Project, position, range},
	crate::prepare_rename,
	tangram_client::prelude::*,
};

impl Project {
	pub(super) fn prepare_rename(
		&self,
		request: &prepare_rename::Request,
	) -> tg::Result<prepare_rename::Response> {
		self.with_language_service(&request.module, |context, service| {
			let info = service.get_rename_info(
				context,
				"",
				&self.uri(&request.module),
				position(request.position),
			);
			let prepare = info.can_rename.then(|| prepare_rename::Prepare {
				placeholder: info.display_name,
				range: range(info.trigger_span),
			});
			let response = prepare_rename::Response { prepare };

			Ok(response)
		})
	}
}
