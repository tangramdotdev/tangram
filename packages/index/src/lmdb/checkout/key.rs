use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Checkout(tg::Id),
	CheckoutDependency {
		checkout: tg::Id,
		dependency: tg::Id,
	},
	DependencyCheckout {
		dependency: tg::Id,
		checkout: tg::Id,
	},
}
