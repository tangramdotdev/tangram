use crate::prelude::*;

pub type Selector = tg::Selector<tg::organization::Id>;

impl From<tg::organization::Id> for Selector {
	fn from(value: tg::organization::Id) -> Self {
		Self::Id(value)
	}
}
