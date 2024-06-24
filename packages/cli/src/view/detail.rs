use super::{
	app::Focus, commands::Commands, data::Data, info::Info, log::Log, tree::NodeKind,
	util::render_block_and_get_area,
};
use either::Either;
use ratatui::prelude::*;
use std::sync::{Arc, RwLock};
use tangram_client as tg;

#[allow(clippy::type_complexity)]
pub struct Detail<H> {
	pub(super) commands: Arc<Commands<H>>,
	info: Arc<Info<H>>,
	data: Option<Either<Arc<Log<H>>, Arc<Data<H>>>>,
	state: RwLock<State>,
}

struct State {
	focus: Focus,
}

impl<H> Detail<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, item: &NodeKind, area: Rect) -> Arc<Self> {
		let commands = Commands::detail();
		let data = match &item {
			NodeKind::Build { build, .. } => Some(Either::Left(Log::new(handle, build, area))),
			NodeKind::Value { value, .. }
				if matches!(
					value,
					tg::Value::Object(tg::Object::File(_) | tg::Object::Leaf(_))
				) =>
			{
				Some(Either::Right(Data::new(handle, value, area)))
			},
			_ => None,
		};
		let info = Info::new(handle, item, area);
		let state = RwLock::new(State { focus: Focus::Tree });
		let info = Arc::new(Self {
			commands,
			info,
			data,
			state,
		});
		info.resize(area);
		info
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		self.info.hit_test(x, y)
			|| self.data.as_ref().map_or(false, |data| match data {
				Either::Left(log) => log.hit_test(x, y),
				Either::Right(data) => data.hit_test(x, y),
			})
	}

	pub fn mouse_scroll(&self, x: u16, y: u16, up: bool) {
		if self.info.hit_test(x, y) {
			if up {
				self.info.up();
			} else {
				self.info.down();
			}
			return;
		}
		let Some(data) = self.data.as_ref() else {
			return;
		};
		match data {
			Either::Left(log) if log.hit_test(x, y) => {
				if up {
					log.up();
				} else {
					log.down();
				}
			},
			Either::Right(data) if data.hit_test(x, y) => {
				if up {
					data.up();
				} else {
					data.down();
				}
			},
			_ => (),
		}
	}

	pub fn resize(&self, area: Rect) {
		if let Some(data) = self.data.as_ref() {
			let layout = Layout::default()
				.direction(Direction::Vertical)
				.constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
				.split(area);
			self.info.resize(layout[0]);
			match data {
				Either::Left(log) => log.resize(layout[1]),
				Either::Right(data) => data.resize(layout[1]),
			}
		} else {
			self.info.resize(area);
		}
	}

	pub fn stop(&self) {
		let Some(Either::Left(log)) = &self.data else {
			return;
		};
		log.stop();
	}

	pub fn top(&self) {
		self.info.top();
	}

	pub fn bottom(&self) {
		self.info.bottom();
	}

	pub fn down(&self) {
		let focus = self.state.read().unwrap().focus;
		match focus {
			Focus::Tree => (),
			Focus::DetailOne => self.info.down(),
			Focus::DetailTwo => match self.data.as_ref() {
				Some(Either::Left(log)) => log.down(),
				Some(Either::Right(data)) => data.down(),
				_ => (),
			},
		};
	}

	pub fn up(&self) {
		let focus = self.state.read().unwrap().focus;
		match focus {
			Focus::Tree => (),
			Focus::DetailOne => self.info.up(),
			Focus::DetailTwo => match self.data.as_ref() {
				Some(Either::Left(log)) => log.up(),
				Some(Either::Right(data)) => data.up(),
				_ => (),
			},
		};
	}

	pub fn set_focus(&self, focus: Focus) {
		self.state.write().unwrap().focus = focus;
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		let state: std::sync::RwLockReadGuard<State> = self.state.read().unwrap();
		if let Some(data) = self.data.as_ref() {
			let layout = Layout::default()
				.direction(Direction::Vertical)
				.constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
				.split(area);
			let info_focus = matches!(state.focus, Focus::DetailOne);
			let info_area = render_block_and_get_area("Info", info_focus, layout[0], buf);
			let data_focus = matches!(state.focus, Focus::DetailTwo);
			let title = if matches!(self.data, Some(Either::Left(_))) {
				"Log"
			} else {
				"Data"
			};
			let data_area = render_block_and_get_area(title, data_focus, layout[1], buf);
			self.info.render(info_area, buf);
			match data {
				Either::Left(log) => log.render(data_area, buf),
				Either::Right(data) => data.render(data_area, buf),
			}
		} else {
			let focus = matches!(state.focus, Focus::DetailOne | Focus::DetailTwo);
			let area = render_block_and_get_area("Info", focus, area, buf);
			self.info.render(area, buf);
		}
	}

	pub fn has_log(&self) -> bool {
		self.data.is_some()
	}
}
