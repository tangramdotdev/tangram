use {
	super::{Project, position, range},
	crate::prepare_rename,
	tangram_client::prelude::*,
};

impl Project {
	pub(super) fn prepare_rename(
		&self,
		arg: &prepare_rename::Arg,
	) -> tg::Result<prepare_rename::Output> {
		self.with_language_service(&arg.module, |context, service| {
			let info = service.get_rename_info(
				context,
				"",
				&self.uri(&arg.module),
				position(arg.position),
			);
			let prepare = info.can_rename.then(|| prepare_rename::Prepare {
				placeholder: info.display_name,
				range: range(info.trigger_span),
			});
			let output = prepare_rename::Output { prepare };

			Ok(output)
		})
	}
}
