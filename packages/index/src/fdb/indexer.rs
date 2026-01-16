use {
	super::Index,
	crate::{
		DeleteTagArg, PutCacheEntryArg, PutObjectArg, PutProcessArg, PutTagArg, TouchObjectArg,
		TouchProcessArg,
	},
	tangram_client::prelude::*,
};

impl Index {
	#[expect(clippy::too_many_arguments)]
	pub async fn handle_messages(
		&self,
		_put_cache_entry_messages: Vec<PutCacheEntryArg>,
		_put_object_messages: Vec<PutObjectArg>,
		_touch_object_messages: Vec<TouchObjectArg>,
		_put_process_messages: Vec<PutProcessArg>,
		_touch_process_messages: Vec<TouchProcessArg>,
		_put_tag_messages: Vec<PutTagArg>,
		_delete_tag_messages: Vec<DeleteTagArg>,
	) -> tg::Result<()> {
		todo!()
	}
}
