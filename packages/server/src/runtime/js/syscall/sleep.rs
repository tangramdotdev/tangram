use super::State;
use std::rc::Rc;
use tangram_client as tg;

pub async fn sleep(_state: Rc<State>, args: (f64,)) -> tg::Result<()> {
	let (duration,) = args;
	let duration = std::time::Duration::from_secs_f64(duration);
	tokio::time::sleep(duration).await;
	Ok(())
}
