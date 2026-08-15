use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Checkout(tg::artifact::Id),
	CheckoutDependency {
		checkout: tg::artifact::Id,
		dependency: tg::artifact::Id,
	},
	DependencyCheckout {
		dependency: tg::artifact::Id,
		checkout: tg::artifact::Id,
	},
}
