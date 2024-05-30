use super::{
	commands::Commands, detail::Detail, tree::Tree, util::render_block_and_get_area, Item,
};
use copypasta::ClipboardProvider;
use crossterm::event::{Event, KeyEvent, MouseEvent, MouseEventKind};
use ratatui::{self as tui, prelude::*};
use std::sync::{
	atomic::{AtomicBool, Ordering},
	Arc, RwLock,
};
use tangram_client as tg;

pub struct App<H> {
	handle: H,
	tree: Arc<Tree<H>>,
	state: RwLock<State<H>>,
	stop: AtomicBool,
}

struct State<H> {
	area: Rect,
	detail_area: Rect,
	detail: Arc<Detail<H>>,
	show_help: bool,
	view: View,
}

#[derive(Copy, Clone, Debug)]
enum View {
	Split(Direction, Selected),
	Tree,
	Detail,
}

#[derive(Copy, Clone, Debug)]
enum Selected {
	Tree,
	Detail,
}

impl<H> App<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, root: Item, rect: tui::layout::Rect) -> Arc<Self> {
		let handle = handle.clone();
		let detail = Detail::new(&handle, &root.clone(), rect);
		let tree = Tree::new(&handle, &[root], rect);
		let stop = AtomicBool::new(false);

		let view = if rect.width >= 80 {
			View::Split(Direction::Horizontal, Selected::Tree)
		} else if rect.height >= 30 {
			View::Split(Direction::Vertical, Selected::Tree)
		} else {
			View::Tree
		};

		let state = RwLock::new(State {
			area: rect,
			detail,
			detail_area: rect,
			show_help: false,
			view,
		});

		let app = Arc::new(Self {
			handle,
			tree,
			state,
			stop,
		});
		app.resize(rect);
		app
	}

	pub fn handle_event(&self, event: &Event) {
		match event {
			Event::Key(event) => self.key(*event),
			Event::Mouse(event) => self.mouse(*event),
			Event::Resize(width, height) => {
				self.resize(tui::layout::Rect::new(0, 0, *width, *height));
			},
			_ => (),
		}
	}

	pub fn key(&self, event: KeyEvent) {
		if self.state.read().unwrap().show_help {
			let commands = Commands::help();
			commands.dispatch(event, self);
		}
		let view = self.state.read().unwrap().view;
		match view {
			View::Split(_, Selected::Tree) | View::Tree => {
				self.tree.commands.dispatch(event, self);
			},
			View::Split(_, Selected::Detail) | View::Detail => {
				let detail = self.state.read().unwrap().detail.clone();
				detail.commands.dispatch(event, self);
			},
		}
	}

	pub fn mouse(&self, event: MouseEvent) {
		let view = self.state.read().unwrap().view;
		match view {
			View::Split(_, Selected::Tree) | View::Tree => match event.kind {
				MouseEventKind::ScrollUp => self.tree.up(),
				MouseEventKind::ScrollDown => self.tree.down(),
				_ => (),
			},
			View::Split(_, Selected::Detail) | View::Detail => {
				let detail = self.state.read().unwrap().detail.clone();
				match event.kind {
					MouseEventKind::ScrollUp => detail.up(),
					MouseEventKind::ScrollDown => detail.down(),
					_ => (),
				}
			},
		}
	}

	pub fn toggle_split(&self) {
		let mut state = self.state.write().unwrap();
		state.view = match state.view {
			View::Split(_, Selected::Tree) => View::Tree,
			View::Split(_, Selected::Detail) => View::Detail,
			View::Tree => {
				let direction = if state.area.height >= 30 {
					Direction::Vertical
				} else {
					Direction::Horizontal
				};
				View::Split(direction, Selected::Tree)
			},
			View::Detail => {
				let direction = if state.area.height >= 30 {
					Direction::Vertical
				} else {
					Direction::Horizontal
				};
				View::Split(direction, Selected::Detail)
			},
		};
	}

	pub fn rotate(&self) {
		let mut state = self.state.write().unwrap();
		if let View::Split(direction, _) = &mut state.view {
			*direction = match direction {
				Direction::Vertical => Direction::Horizontal,
				Direction::Horizontal => Direction::Vertical,
			};
		};
	}

	pub fn toggle_help(&self) {
		let mut state = self.state.write().unwrap();
		state.show_help = !state.show_help;
	}

	pub fn top(&self) {
		let mut state = self.state.write().unwrap();
		match state.view {
			View::Split(_, Selected::Tree) | View::Tree => {
				self.tree.top();
				let selected = self.tree.get_selected();
				state.detail = Detail::new(&self.handle, &selected, state.detail_area);
			},
			View::Split(_, Selected::Detail) | View::Detail => {
				state.detail.top();
			},
		}
	}

	pub fn bottom(&self) {
		let mut state = self.state.write().unwrap();
		match state.view {
			View::Split(_, Selected::Tree) | View::Tree => {
				self.tree.bottom();
				let selected = self.tree.get_selected();
				state.detail = Detail::new(&self.handle, &selected, state.detail_area);
			},
			View::Split(_, Selected::Detail) | View::Detail => {
				state.detail.bottom();
			},
		}
	}

	pub fn up(&self) {
		let mut state = self.state.write().unwrap();
		match state.view {
			View::Split(_, Selected::Tree) | View::Tree => {
				self.tree.up();
				let selected = self.tree.get_selected();
				state.detail = Detail::new(&self.handle, &selected, state.detail_area);
			},
			View::Split(_, Selected::Detail) | View::Detail => {
				state.detail.up();
			},
		}
	}

	pub fn down(&self) {
		let mut state = self.state.write().unwrap();
		match state.view {
			View::Split(_, Selected::Tree) | View::Tree => {
				self.tree.down();
				let selected = self.tree.get_selected();
				state.detail = Detail::new(&self.handle, &selected, state.detail_area);
			},
			View::Split(_, Selected::Detail) | View::Detail => {
				state.detail.down();
			},
		}
	}

	pub fn enter(&self) {
		let mut state = self.state.write().unwrap();
		state.view = match state.view {
			View::Split(direction, Selected::Tree) => View::Split(direction, Selected::Detail),
			View::Tree => View::Detail,
			_ => return,
		};
	}

	pub fn back(&self) {
		let mut state = self.state.write().unwrap();
		state.view = match state.view {
			View::Split(direction, Selected::Detail) => View::Split(direction, Selected::Tree),
			View::Detail => View::Tree,
			_ => return,
		};
	}

	pub fn tab(&self) {
		let mut state = self.state.write().unwrap();
		state.view = match state.view {
			View::Split(direction, Selected::Tree) => View::Split(direction, Selected::Detail),
			View::Split(direction, Selected::Detail) => View::Split(direction, Selected::Tree),
			View::Tree => View::Detail,
			View::Detail => View::Tree,
		};
	}

	pub fn set_tab(&self, n: usize) {
		let state = self.state.read().unwrap();
		state.detail.set_tab(n);
	}

	pub fn resize(&self, rect: Rect) {
		let mut state = self.state.write().unwrap();
		let layout = Layout::default()
			.direction(Direction::Vertical)
			.constraints([Constraint::Fill(1), Constraint::Max(1)]);
		let rects = layout.split(rect);
		let view_area = rects[0];
		if let View::Split(direction, _) = state.view {
			let layout = Layout::default()
				.direction(direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)]);
			let rects = layout.split(view_area);
			let (tree_area, detail_area) = (rects[0], rects[1]);
			self.tree.resize(tree_area);
			state.detail.resize(detail_area);
			state.detail_area = detail_area;
		} else {
			self.tree.resize(view_area);
			state.detail_area = view_area;
		}
	}

	pub fn cancel(&self) {
		let Item::Build { build, remote } = self.tree.get_selected() else {
			return;
		};
		let client = self.handle.clone();
		tokio::spawn(async move {
			let outcome = tg::build::outcome::Data::Canceled;
			let arg = tg::build::finish::Arg { outcome, remote };
			build.finish(&client, arg).await.ok();
		});
	}

	pub fn quit(&self) {
		self.stop();
	}

	pub fn expand_children(&self) {
		if let Item::Build { .. } = self.tree.get_selected() {
			self.tree.expand_build_children();
			self.tree.expand_object_children();
		} else {
			self.tree.expand_object_children();
		}
	}

	pub fn collapse_children(&self) {
		self.tree.collapse_children();
	}

	pub fn copy_selected_to_clipboard(&self) {
		let Ok(mut context) = copypasta::ClipboardContext::new() else {
			return;
		};
		let selected = self.tree.get_selected();
		let text = match selected {
			Item::Root => return,
			Item::Build { build, .. } => build.id().to_string(),
			Item::Value { value, .. } => value.to_string(),
			Item::Package { dependency, .. } => dependency.to_string(),
		};
		context.set_contents(text).ok();
	}

	pub fn push(&self) {
		self.tree.push();
	}

	pub fn pop(&self) {
		self.tree.pop();
	}

	pub fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let state = self.state.read().unwrap();
		let commands = match state.view {
			View::Split(_, Selected::Tree) | View::Tree => self.tree.commands.clone(),
			View::Split(_, Selected::Detail) | View::Detail => state.detail.commands.clone(),
		};

		if state.show_help {
			let area = render_block_and_get_area("Help", true, rect, buf);
			commands.render_full(area, buf);
			return;
		}

		let layout = Layout::default()
			.direction(Direction::Vertical)
			.constraints([Constraint::Fill(1), Constraint::Max(1)]);
		let rects = layout.split(rect);
		let (view_area, help_area) = (rects[0], rects[1]);
		commands.render_short(help_area, buf);

		match state.view {
			View::Split(direction, selected) => {
				let layout = Layout::default()
					.direction(direction)
					.constraints([Constraint::Fill(1), Constraint::Fill(1)]);
				let rects = layout.split(view_area);
				let (tree_area, detail_area) = (rects[0], rects[1]);

				let tree_area = render_block_and_get_area(
					"Tree",
					matches!(selected, Selected::Tree),
					tree_area,
					buf,
				);
				self.tree.render(tree_area, buf);

				let detail_area = render_block_and_get_area(
					"Detail",
					matches!(selected, Selected::Detail),
					detail_area,
					buf,
				);
				state.detail.render(detail_area, buf);
			},
			View::Detail => {
				let view_area = render_block_and_get_area("Detail", true, view_area, buf);
				state.detail.render(view_area, buf);
			},
			View::Tree => {
				let view_area = render_block_and_get_area("Tree", true, view_area, buf);
				self.tree.render(view_area, buf);
			},
		}
	}

	pub fn stop(&self) {
		let state = self.state.read().unwrap();
		self.stop.store(true, Ordering::Release);
		self.tree.stop();
		state.detail.stop();
	}

	pub fn stopped(&self) -> bool {
		self.stop.load(Ordering::Acquire)
	}

	pub async fn wait(&self) {
		self.tree.wait().await;
	}
}
