use {
	super::State,
	std::{rc::Rc, time::Duration},
	tangram_client::prelude::*,
};

pub async fn sleep(_state: Rc<State>, args: (f64,)) -> tg::Result<()> {
	let (duration,) = args;
	let duration = Duration::from_secs_f64(duration);
	tokio::time::sleep(duration).await;
	Ok(())
}
