use super::Item;
use num::ToPrimitive;
use ratatui::{self as tui, prelude::*};
use std::sync::{Arc, RwLock};
use tangram_client as tg;

pub struct Info<H> {
	handle: H,
	state: RwLock<State>,
}

pub struct State {
	area: Rect,
	scroll: usize,
	num_lines: usize,
	view: Option<Box<dyn InfoViewExt>>,
}

impl<H> Info<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, item: &Item, area: Rect) -> Arc<Self> {
		let state = State {
			area,
			scroll: 0,
			num_lines: 0,
			view: None,
		};
		let info = Arc::new(Self {
			handle: handle.clone(),
			state: RwLock::new(state),
		});
		tokio::spawn({
			let info = info.clone();
			let item = item.clone();
			async move {
				let view = match item {
					Item::Root => unreachable!(),
					Item::Build { build, .. } => match info.handle.get_build(build.id()).await {
						Ok(output) => Box::new(output) as Box<dyn InfoViewExt>,
						Err(error) => Box::new(error) as Box<dyn InfoViewExt>,
					},
					Item::Package {
						dependency,
						lock,
						path,
						..
					} => {
						let dependency = path.map_or(dependency, tg::Dependency::with_path);
						match info
							.handle
							.get_package(
								&dependency,
								tg::package::get::Arg {
									path: true,
									lock: false,
									metadata: true,
									dependencies: false,
									locked: true,
									yanked: true,
								},
							)
							.await
						{
							Ok(mut output) => {
								output.lock.replace(lock.id(&info.handle).await.unwrap());
								Box::new(output) as Box<dyn InfoViewExt>
							},
							Err(error) => Box::new(error) as Box<dyn InfoViewExt>,
						}
					},

					Item::Value {
						value: tg::Value::Object(object),
						..
					} => match object.data(&info.handle).await {
						Ok(tg::object::Data::File(data)) => Box::new(data) as Box<dyn InfoViewExt>,
						Ok(tg::object::Data::Directory(data)) => {
							Box::new(data) as Box<dyn InfoViewExt>
						},
						Ok(tg::object::Data::Symlink(data)) => {
							Box::new(data) as Box<dyn InfoViewExt>
						},
						Ok(tg::object::Data::Target(data)) => {
							Box::new(data) as Box<dyn InfoViewExt>
						},
						Ok(tg::object::Data::Branch(data)) => {
							Box::new(data) as Box<dyn InfoViewExt>
						},
						Ok(tg::object::Data::Leaf(data)) => Box::new(data) as Box<dyn InfoViewExt>,
						Ok(tg::object::Data::Lock(data)) => Box::new(data) as Box<dyn InfoViewExt>,
						Err(error) => Box::new(error) as Box<dyn InfoViewExt>,
					},
					Item::Value { value, .. } => match value.data(&info.handle).await {
						Ok(data) => Box::new(data) as Box<dyn InfoViewExt>,
						Err(error) => Box::new(error) as Box<dyn InfoViewExt>,
					},
				};
				info.state.write().unwrap().view.replace(view);
			}
		});

		info
	}

	pub fn resize(&self, area: Rect) {
		self.state.write().unwrap().area = area;
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		let state = self.state.read().unwrap();
		let position = tui::layout::Position::new(x, y);
		state.area.contains(position)
	}

	pub fn bottom(&self) {
		let mut state = self.state.write().unwrap();
		let height = state.area.height.to_usize().unwrap();
		let max_scroll = state.num_lines.saturating_sub(height);
		state.scroll = max_scroll;
	}

	pub fn top(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = 0;
	}

	pub fn down(&self) {
		let mut state = self.state.write().unwrap();
		let height = state.area.height.to_usize().unwrap();
		let max_scroll = state.num_lines.saturating_sub(height);
		state.scroll = (state.scroll + 1).min(max_scroll);
	}

	pub fn up(&self) {
		let mut state = self.state.write().unwrap();
		state.scroll = state.scroll.saturating_sub(1);
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		let mut state = self.state.write().unwrap();
		if let Some(view) = &state.view {
			let len = view.render(state.scroll, area, buf);
			state.num_lines = len;
		}
	}
}

trait InfoViewExt
where
	Self: Send + Sync + 'static,
{
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize;
}

impl InfoViewExt for tg::build::get::Output {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = vec![
			("id", self.id.to_string()),
			("host", self.host.clone()),
			("retry", self.retry.to_string()),
			("status", self.status.to_string()),
			("target", self.target.to_string()),
		];
		if let Some(count) = self.count {
			rows.push(("count", count.to_string()));
		}
		if let Some(log) = &self.log {
			rows.push(("log", log.to_string()));
		}
		if let Some(logs_count) = self.logs_weight {
			rows.push(("logs_count", logs_count.to_string()));
		}
		if let Some(logs_weight) = self.logs_weight {
			rows.push(("logs_weight", logs_weight.to_string()));
		}
		if let Some(outcome) = &self.outcome {
			rows.push(("outcome", serde_json::to_string(outcome).unwrap()));
		}
		if let Some(outcomes_count) = self.outcomes_count {
			rows.push(("outcomes_count", outcomes_count.to_string()));
		}
		if let Some(outcomes_weight) = self.outcomes_weight {
			rows.push(("outcomes_weight", outcomes_weight.to_string()));
		}
		if let Some(targets_count) = self.targets_count {
			rows.push(("targets_count", targets_count.to_string()));
		}
		if let Some(targets_weight) = self.targets_weight {
			rows.push(("targets_weight", targets_weight.to_string()));
		}
		rows.push(("created_at", self.created_at.to_string()));
		if let Some(dequeued_at) = &self.dequeued_at {
			rows.push(("dequeued_at", dequeued_at.to_string()));
		}
		if let Some(started_at) = &self.started_at {
			rows.push(("started_at", started_at.to_string()));
		}
		if let Some(finished_at) = &self.finished_at {
			rows.push(("finished_at", finished_at.to_string()));
		}

		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(20), Constraint::Percentage(80)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::package::get::Output {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = vec![("artifact", self.artifact.to_string())];
		if let Some(lock) = &self.lock {
			rows.push(("lock", lock.to_string()));
		}
		if let Some(metadata) = &self.metadata {
			if let Some(name) = &metadata.name {
				rows.push(("name", name.clone()));
			}
			if let Some(version) = &metadata.version {
				rows.push(("version", version.clone()));
			}
			if let Some(description) = &metadata.description {
				rows.push(("description", description.clone()));
			}
		}
		if let Some(path) = &self.path {
			rows.push(("path", path.to_string()));
		}
		if let Some(yanked) = self.yanked {
			rows.push(("yanked", yanked.to_string()));
		}

		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(20), Constraint::Percentage(80)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::value::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let text = match self {
			tg::value::Data::Null => "null".into(),
			tg::value::Data::Bool(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Number(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::String(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Array(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Map(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Object(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Bytes(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Path(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Mutation(value) => serde_json::to_string_pretty(value).unwrap(),
			tg::value::Data::Template(value) => serde_json::to_string_pretty(value).unwrap(),
		};
		let len = text.lines().count();
		let lines = text
			.lines()
			.skip(scroll)
			.map(tui::text::Line::raw)
			.collect::<Vec<_>>();
		tui::widgets::Paragraph::new(lines)
			.wrap(tui::widgets::Wrap { trim: false })
			.render(area, buf);
		len
	}
}

impl InfoViewExt for tg::Error {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let text = self.to_string();
		let len = text.lines().count();
		let lines = text
			.lines()
			.skip(scroll)
			.map(tui::text::Line::raw)
			.collect::<Vec<_>>();
		tui::widgets::Paragraph::new(lines)
			.wrap(tui::widgets::Wrap { trim: false })
			.render(area, buf);
		len
	}
}

impl InfoViewExt for tg::file::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = vec![
			("contents", self.contents.to_string()),
			("executable", self.executable.to_string()),
			("references", String::new()),
		];
		for reference in &self.references {
			rows.push(("", reference.to_string()));
		}
		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(20), Constraint::Percentage(80)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::directory::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = Vec::new();
		for (name, artifact) in &self.entries {
			rows.push((name, artifact.to_string()));
		}
		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(50), Constraint::Percentage(50)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::symlink::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = vec![];
		if let Some(artifact) = &self.artifact {
			rows.push(("artifact", artifact.to_string()));
		}
		if let Some(path) = &self.path {
			rows.push(("path", path.to_string()));
		}
		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(20), Constraint::Percentage(80)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::target::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = vec![
			("host", self.host.clone()),
			("args", serde_json::to_string(&self.args).unwrap()),
			("env", serde_json::to_string(&self.env).unwrap()),
		];
		if let Some(executable) = &self.executable {
			rows.push(("executable", executable.to_string()));
		}
		if let Some(checksum) = &self.checksum {
			rows.push(("checksum", checksum.to_string()));
		}
		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(20), Constraint::Percentage(80)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::branch::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let mut rows = vec![("children", String::new())];
		for child in self.children() {
			rows.push(("", child.to_string()));
		}
		let len = rows.len();
		let rows = rows
			.into_iter()
			.map(|(name, value)| {
				let cells = vec![
					tui::widgets::Cell::new(Text::raw(name).left_aligned()),
					tui::widgets::Cell::new(Text::raw(value).left_aligned()),
				];
				tui::widgets::Row::new(cells)
			})
			.skip(scroll);
		let widths = [Constraint::Percentage(20), Constraint::Percentage(80)];
		let table = tui::widgets::Table::new(rows, widths);
		<tui::widgets::Table as tui::widgets::Widget>::render(table, area, buf);
		len
	}
}

impl InfoViewExt for tg::leaf::Data {
	fn render(&self, _scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		tui::widgets::Paragraph::new("(bytes)").render(area, buf);
		1
	}
}

impl InfoViewExt for tg::lock::Data {
	fn render(&self, scroll: usize, area: Rect, buf: &mut Buffer) -> usize {
		let text = serde_json::to_string_pretty(self).unwrap();
		let len = text.lines().count();
		let lines = text
			.lines()
			.skip(scroll)
			.map(tui::text::Line::raw)
			.collect::<Vec<_>>();
		tui::widgets::Paragraph::new(lines)
			.wrap(tui::widgets::Wrap { trim: false })
			.render(area, buf);
		len
	}
}
