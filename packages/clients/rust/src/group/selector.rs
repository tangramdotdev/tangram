use crate::prelude::*;

pub type Selector = tg::Selector<tg::group::Id>;

impl From<tg::group::Id> for Selector {
	fn from(value: tg::group::Id) -> Self {
		Self::Id(value)
	}
}
