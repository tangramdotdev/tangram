use super::{info::Info, log::Log};
use either::Either;
use ratatui::{
	prelude::*,
	widgets::{Paragraph, Tabs, Wrap},
};
use std::sync::{Arc, RwLock};
use tangram_client as tg;

pub struct Detail<H> {
	info: Arc<Data<H>>,
	data: Either<Arc<Log<H>>, Arc<Info<H>>>,
	state: RwLock<State>,
}

struct State {
	selected_tab: usize,
}

impl<H> Detail<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, object: Either<tg::Build, tg::Value>, area: Rect) -> Arc<Self> {
		let info = Data::new(handle, &object, area);
		let data = object
			.map_left(|build| Log::new(handle, &build, area))
			.map_right(|value| Info::new(handle, &value, area));
		let state = RwLock::new(State { selected_tab: 0 });
		Arc::new(Self { info, data, state })
	}

	pub fn resize(&self, area: Rect) {
		self.info.resize(area);
		match &self.data {
			Either::Left(log) => log.resize(area),
			Either::Right(info) => info.resize(area),
		};
	}

	pub fn stop(&self) {
		let Either::Left(log) = &self.data else {
			return;
		};
		log.stop();
	}

	pub fn down(&self) {
		let tab = self.state.read().unwrap().selected_tab;
		if tab == 0 {
			self.info.down();
		} else {
			match &self.data {
				Either::Left(log) => log.down(),
				Either::Right(info) => info.down(),
			}
		}
	}

	pub fn up(&self) {
		let tab = self.state.read().unwrap().selected_tab;
		if tab == 0 {
			self.info.up();
		} else {
			match &self.data {
				Either::Left(log) => log.up(),
				Either::Right(info) => info.up(),
			}
		}
	}

	pub fn tab(&self) {
		let mut state = self.state.write().unwrap();
		state.selected_tab = (state.selected_tab + 1) % 2;
	}

	pub fn set_tab(&self, n: usize) {
		let mut state = self.state.write().unwrap();
		state.selected_tab = n;
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		let state = self.state.read().unwrap();

		let layout = Layout::default()
			.direction(Direction::Vertical)
			.constraints([Constraint::Max(1), Constraint::Fill(1)]);
		let rects = layout.split(area);
		let (tab_area, view_area) = (rects[0], rects[1]);
		let titles = [
			"Info (1)",
			if self.data.is_left() {
				"Log (2)"
			} else {
				"Data (2)"
			},
		];

		Tabs::new(titles)
			.select(state.selected_tab)
			.divider(" ")
			.render(tab_area, buf);

		match state.selected_tab {
			0 => self.info.render(view_area, buf),
			1 => match &self.data {
				Either::Left(data) => data.render(view_area, buf),
				Either::Right(data) => data.render(view_area, buf),
			},
			_ => unreachable!(),
		}
	}
}

pub struct Data<H> {
	handle: H,
	value: Either<tg::Build, tg::Value>,
	state: RwLock<DataState>,
}

struct DataState {
	area: Rect,
	text: String,
	scroll: usize,
}

impl<H> Data<H>
where
	H: tg::Handle,
{
	fn new(handle: &H, value: &Either<tg::Build, tg::Value>, area: Rect) -> Arc<Self> {
		let handle = handle.clone();
		let state = RwLock::new(DataState {
			area,
			text: String::new(),
			scroll: 0,
		});
		let value = value.clone();
		let data = Arc::new(Self {
			handle,
			value,
			state,
		});
		tokio::task::spawn({
			let data = data.clone();
			async move {
				let text = data
					.get_text()
					.await
					.unwrap_or_else(|error| format!("error: {error}"));
				data.state.write().unwrap().text = text;
			}
		});
		data
	}

	fn resize(&self, area: Rect) {
		let mut state = self.state.write().unwrap();
		state.area = area;
	}

	fn down(&self) {
		let mut state = self.state.write().unwrap();
		let num_lines = state.text.lines().count();
		state.scroll = (state.scroll + 1).min(num_lines);
	}

	fn up(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = state.scroll.saturating_sub(1);
	}

	fn render(&self, area: Rect, buf: &mut Buffer) {
		let state = self.state.read().unwrap();
		let lines = state
			.text
			.lines()
			.skip(state.scroll)
			.map(Line::raw)
			.collect::<Vec<_>>();
		Paragraph::new(lines)
			.wrap(Wrap { trim: false })
			.render(area, buf);
	}

	async fn get_text(&self) -> tg::Result<String> {
		match &self.value {
			Either::Left(build) => {
				let info = self.handle.get_build(build.id()).await?;
				Ok(serde_json::to_string_pretty(&info).unwrap())
			},
			Either::Right(tg::Value::Object(tg::Object::Leaf(_))) => Ok("(bytes)".into()),
			Either::Right(tg::Value::Object(tg::Object::Branch(object))) => {
				let data = object.data(&self.handle, None).await?;
				Ok(serde_json::to_string_pretty(&data).unwrap())
			},
			Either::Right(tg::Value::Object(tg::Object::Directory(object))) => {
				let data = object.data(&self.handle, None).await?;
				Ok(serde_json::to_string_pretty(&data).unwrap())
			},
			Either::Right(tg::Value::Object(tg::Object::File(object))) => {
				let data = object.data(&self.handle, None).await?;
				Ok(serde_json::to_string_pretty(&data).unwrap())
			},
			Either::Right(tg::Value::Object(tg::Object::Symlink(object))) => {
				let data = object.data(&self.handle, None).await?;
				Ok(serde_json::to_string_pretty(&data).unwrap())
			},
			Either::Right(tg::Value::Object(tg::Object::Target(object))) => {
				let data = object.data(&self.handle, None).await?;
				Ok(serde_json::to_string_pretty(&data).unwrap())
			},
			Either::Right(tg::Value::Object(tg::Object::Lock(object))) => {
				let data = object.data(&self.handle, None).await?;
				Ok(serde_json::to_string_pretty(&data).unwrap())
			},
			Either::Right(value) => {
				let data = value.data(&self.handle, None).await?;
				match &data {
					tg::value::Data::Null => Ok("null".into()),
					tg::value::Data::Bool(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Number(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::String(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Array(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Map(value) => Ok(serde_json::to_string_pretty(value).unwrap()),
					tg::value::Data::Object(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Bytes(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Path(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Mutation(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
					tg::value::Data::Template(value) => {
						Ok(serde_json::to_string_pretty(value).unwrap())
					},
				}
			},
		}
	}
}
