use crate::prelude::*;

pub type Selector = tg::Selector<tg::tag::Id>;

impl From<tg::tag::Id> for Selector {
	fn from(value: tg::tag::Id) -> Self {
		Self::Id(value)
	}
}
