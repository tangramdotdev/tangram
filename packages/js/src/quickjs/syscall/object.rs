use {
	super::Result,
	crate::quickjs::{StateHandle, serde::Serde},
	rquickjs as qjs,
	tangram_client::prelude::*,
};

pub async fn get(ctx: qjs::Ctx<'_>, id: Serde<tg::object::Id>) -> Result<Serde<tg::object::Data>> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let Serde(id) = id;
	let result = async {
		let data = state
			.main_runtime_handle
			.spawn({
				let handle = state.handle.clone();
				let id = id.clone();
				async move {
					let tg::object::get::Output { bytes } = handle.get_object(&id).await?;
					let data = tg::object::Data::deserialize(id.kind(), bytes)?;
					Ok::<_, tg::Error>(data)
				}
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?
			.map_err(|source| tg::error!(!source, "failed to get the object"))?;
		Ok(data)
	}
	.await;
	Result(result.map(Serde))
}

pub fn id(data: Serde<tg::object::Data>) -> Result<Serde<tg::object::Id>> {
	let Serde(data) = data;
	let result = (|| {
		let bytes = data.serialize()?;
		let id = tg::object::Id::new(data.kind(), &bytes);
		Ok(id)
	})();
	Result(result.map(Serde))
}
