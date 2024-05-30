use super::{commands::Commands, data::Data, log::Log, Item};
use either::Either;
use num::ToPrimitive;
use ratatui::{
	prelude::*,
	widgets::{Paragraph, Tabs, Wrap},
};
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
	selected_tab: usize,
}

impl<H> Detail<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, item: &Item, area: Rect) -> Arc<Self> {
		let commands = Commands::detail();
		let data = match &item {
			Item::Build { build, .. } => Some(Either::Left(Log::new(handle, build, area))),
			Item::Value { value, .. }
				if matches!(
					value,
					tg::Value::Object(tg::Object::Leaf(_) | tg::Object::File(_))
				) =>
			{
				Some(Either::Right(Data::new(handle, value, area)))
			},
			_ => None,
		};

		let info = Info::new(handle, item, area);

		let state = RwLock::new(State { selected_tab: 0 });
		Arc::new(Self {
			commands,
			info,
			data,
			state,
		})
	}

	pub fn resize(&self, area: Rect) {
		self.info.resize(area);
		match &self.data {
			Some(Either::Left(log)) => log.resize(area),
			Some(Either::Right(info)) => info.resize(area),
			None => (),
		};
	}

	pub fn stop(&self) {
		let Some(Either::Left(log)) = &self.data else {
			return;
		};
		log.stop();
	}

	pub fn top(&self) {
		let tab = self.state.read().unwrap().selected_tab;
		if tab == 0 {
			self.info.top();
		} else if let Some(data) = &self.data {
			match data {
				Either::Left(_) => (),
				Either::Right(data) => data.top(),
			}
		}
	}

	pub fn bottom(&self) {
		let tab = self.state.read().unwrap().selected_tab;
		if tab == 0 {
			self.info.bottom();
		} else if let Some(data) = &self.data {
			match data {
				Either::Left(_) => (),
				Either::Right(data) => data.bottom(),
			}
		}
	}

	pub fn down(&self) {
		let tab = self.state.read().unwrap().selected_tab;
		if tab == 0 {
			self.info.down();
		} else if let Some(data) = &self.data {
			match data {
				Either::Left(log) => log.down(),
				Either::Right(info) => info.down(),
			}
		}
	}

	pub fn up(&self) {
		let tab = self.state.read().unwrap().selected_tab;
		if tab == 0 {
			self.info.up();
		} else if let Some(data) = &self.data {
			match data {
				Either::Left(log) => log.up(),
				Either::Right(info) => info.up(),
			}
		}
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

		match &self.data {
			Some(Either::Left(log)) => {
				let titles = ["Info (1)", "Log (2)"];
				Tabs::new(titles)
					.select(state.selected_tab)
					.divider(" ")
					.render(tab_area, buf);
				match state.selected_tab {
					0 => self.info.render(view_area, buf),
					1 => log.render(view_area, buf),
					_ => unreachable!(),
				}
			},
			Some(Either::Right(data)) => {
				let titles = ["Info (1)", "Data (2)"];
				Tabs::new(titles)
					.select(state.selected_tab)
					.divider(" ")
					.render(tab_area, buf);
				match state.selected_tab {
					0 => self.info.render(view_area, buf),
					1 => data.render(view_area, buf),
					_ => unreachable!(),
				}
			},
			None => {
				let titles = ["Info (1)"];
				Tabs::new(titles)
					.select(0)
					.divider(" ")
					.render(tab_area, buf);
				self.info.render(view_area, buf);
			},
		}
	}
}

pub struct Info<H> {
	handle: H,
	value: Item,
	state: RwLock<DataState>,
}

struct DataState {
	area: Rect,
	text: String,
	scroll: usize,
}

impl<H> Info<H>
where
	H: tg::Handle,
{
	fn new(handle: &H, item: &Item, area: Rect) -> Arc<Self> {
		let handle = handle.clone();
		let state = RwLock::new(DataState {
			area,
			text: String::new(),
			scroll: 0,
		});
		let value = item.clone();
		let info = Arc::new(Self {
			handle,
			value,
			state,
		});
		tokio::task::spawn({
			let data = info.clone();
			async move {
				let value = data
					.get_value()
					.await
					.ok()
					.map(|value| serde_json::to_string_pretty(&value).unwrap());
				let text = value.unwrap_or("missing".into());
				data.state.write().unwrap().text = text;
			}
		});
		info
	}

	fn resize(&self, area: Rect) {
		let mut state = self.state.write().unwrap();
		state.area = area;
	}

	fn bottom(&self) {
		let mut state = self.state.write().unwrap();
		let num_lines = state.text.lines().count();
		state.scroll =
			num_lines.saturating_sub(state.area.height.to_usize().unwrap().saturating_sub(2));
	}

	fn top(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = 0;
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

	async fn get_value(&self) -> tg::Result<serde_json::Value> {
		match &self.value {
			Item::Root => Ok(serde_json::Value::Null),
			Item::Build { build, .. } => {
				let info = self.handle.get_build(build.id()).await?;
				Ok(serde_json::to_value(&info).unwrap())
			},
			Item::Value {
				value: tg::Value::Object(object),
				..
			} => self.get_object_value(object).await,
			Item::Value { value, .. } => {
				let data = value.data(&self.handle).await?;
				match &data {
					tg::value::Data::Null => Ok("null".into()),
					tg::value::Data::Bool(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Number(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::String(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Array(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Map(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Object(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Bytes(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Path(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Mutation(value) => Ok(serde_json::to_value(value).unwrap()),
					tg::value::Data::Template(value) => Ok(serde_json::to_value(value).unwrap()),
				}
			},
			Item::Package {
				dependency,
				artifact,
				lock,
			} => {
				#[derive(serde::Serialize)]
				struct Data {
					dependency: tg::Dependency,
					#[serde(skip_serializing_if = "Option::is_none")]
					artifact: Option<tg::artifact::Id>,
					lock: tg::lock::Id,
				}
				let artifact = if let Some(artifact) = artifact {
					Some(artifact.id(&self.handle).await?)
				} else {
					None
				};
				let lock = lock.id(&self.handle).await?;
				let data = Data {
					dependency: dependency.clone(),
					artifact,
					lock,
				};
				Ok(serde_json::to_value(data).unwrap())
			},
		}
	}

	async fn get_object_value(&self, object: &tg::Object) -> tg::Result<serde_json::Value> {
		let id = object.id(&self.handle).await?;
		let output = self.handle.get_object(&id).await?;
		let data = match object {
			tg::Object::Branch(object) => {
				let data = object.data(&self.handle).await?;
				serde_json::to_value(data).unwrap()
			},
			tg::Object::Leaf(_) => serde_json::to_value("(bytes)").unwrap(),
			tg::Object::Directory(object) => {
				let data = object.data(&self.handle).await?;
				serde_json::to_value(data).unwrap()
			},
			tg::Object::File(object) => {
				let data = object.data(&self.handle).await?;
				serde_json::to_value(data).unwrap()
			},
			tg::Object::Symlink(object) => {
				let data = object.data(&self.handle).await?;
				serde_json::to_value(data).unwrap()
			},
			tg::Object::Target(object) => {
				let data = object.data(&self.handle).await?;
				serde_json::to_value(data).unwrap()
			},
			tg::Object::Lock(object) => {
				let data = object.data(&self.handle).await?;
				serde_json::to_value(data).unwrap()
			},
		};

		#[derive(serde::Serialize)]
		struct Info {
			id: tg::object::Id,
			#[serde(skip_serializing_if = "Option::is_none")]
			count: Option<u64>,
			#[serde(skip_serializing_if = "Option::is_none")]
			weight: Option<u64>,
			data: serde_json::Value,
		}

		let info = Info {
			id,
			count: output.metadata.count,
			weight: output.metadata.weight,
			data,
		};

		Ok(serde_json::to_value(&info).unwrap())
	}
}
