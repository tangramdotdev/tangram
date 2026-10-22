use crate::prelude::*;

impl tg::instance::Clock for tg::Session {
	fn try_get_clock(&self) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		self.try_get_clock()
	}

	fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		self.try_set_clock(arg)
	}

	fn try_advance_clock(
		&self,
		arg: tg::clock::advance::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::clock::get::Output>>> {
		self.try_advance_clock(arg)
	}
}
