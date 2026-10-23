use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub struct ConnectedEvent {
	pub lease: tg::process::Lease,
	pub process: tg::Referent<tg::process::Id>,
}
