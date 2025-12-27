use {
	super::Result, crate::quickjs::StateHandle, rquickjs as qjs, std::time::Duration,
	tangram_client::prelude::*,
};

pub async fn sleep(ctx: qjs::Ctx<'_>, duration: f64) -> Result<()> {
	let state = ctx.userdata::<StateHandle>().unwrap().clone();
	let result = async {
		state
			.main_runtime_handle
			.spawn(async move {
				let duration = Duration::from_secs_f64(duration);
				tokio::time::sleep(duration).await;
			})
			.await
			.map_err(|source| tg::error!(!source, "the task panicked"))?;
		Ok(())
	}
	.await;
	Result(result)
}
