use super::{
	commands::Commands, detail::Detail, tree::Tree, util::render_block_and_get_area, Item,
};
use copypasta::ClipboardProvider;
use crossterm::event::{Event, KeyEvent, MouseEvent, MouseEventKind};
use ratatui as tui;
use std::sync::{
	atomic::{AtomicBool, Ordering},
	Arc, RwLock,
};
use tangram_client as tg;
use tui::prelude::*;

pub struct App<H> {
	handle: H,
	commands: Arc<Commands<H>>,
	tree: Arc<Tree<H>>,
	state: RwLock<State<H>>,
	stop: AtomicBool,
}

struct State<H> {
	detail_area: Rect,
	detail: Arc<Detail<H>>,
	show_help: bool,
	show_detail: bool,
	split: bool,
	direction: Direction,
}

impl<H> App<H>
where
	H: tg::Handle,
{
	pub fn new(handle: &H, root: Item, rect: tui::layout::Rect) -> Arc<Self> {
		let handle = handle.clone();
		let commands = Commands::new();
		let detail = Detail::new(&handle, &root.clone(), rect);
		let tree = Tree::new(&handle, &[root], rect);
		let stop = AtomicBool::new(false);
		let (split, direction) = if rect.width >= 80 {
			(true, Direction::Horizontal)
		} else if rect.height >= 30 {
			(true, Direction::Vertical)
		} else {
			(false, Direction::Horizontal)
		};

		let state = RwLock::new(State {
			detail,
			detail_area: rect,
			show_help: false,
			show_detail: false,
			split,
			direction,
		});

		let app = Arc::new(Self {
			handle,
			commands,
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
		self.commands.dispatch(event, self);
	}

	pub fn mouse(&self, event: MouseEvent) {
		let state = self.state.read().unwrap();
		if state.split || state.show_detail {
			match event.kind {
				MouseEventKind::ScrollDown => state.detail.down(),
				MouseEventKind::ScrollUp => state.detail.up(),
				_ => (),
			}
		}
	}

	pub fn toggle_split(&self) {
		let mut state = self.state.write().unwrap();
		state.split = !state.split;
	}

	pub fn rotate(&self) {
		let mut state = self.state.write().unwrap();
		state.direction = match state.direction {
			Direction::Vertical => Direction::Horizontal,
			Direction::Horizontal => Direction::Vertical,
		}
	}

	pub fn toggle_help(&self) {
		let mut state = self.state.write().unwrap();
		state.show_help = !state.show_help;
	}

	pub fn show_detail(&self) {
		let mut state = self.state.write().unwrap();
		state.show_detail = true;
	}

	pub fn show_tree(&self) {
		let mut state = self.state.write().unwrap();
		state.show_detail = false;
		state.show_help = false;
	}

	pub fn tab(&self) {
		let state = self.state.read().unwrap();
		if state.show_detail || state.split {
			state.detail.tab();
		}
	}

	pub fn set_tab(&self, n: usize) {
		let state = self.state.read().unwrap();
		if state.show_detail || state.split {
			state.detail.set_tab(n);
		}
	}

	pub fn resize(&self, rect: Rect) {
		let mut state = self.state.write().unwrap();
		let layout = Layout::default()
			.direction(Direction::Vertical)
			.constraints([Constraint::Fill(1), Constraint::Max(1)]);
		let rects = layout.split(rect);
		let view_area = rects[0];
		if state.split {
			let layout = Layout::default()
				.direction(state.direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)]);
			let rects = layout.split(view_area);
			let (tree_area, detail_area) = (rects[0], rects[1]);
			self.tree.resize(tree_area);
			state.detail_area = detail_area;
		} else {
			self.tree.resize(view_area);
			state.detail_area = view_area;
		}
		state.detail.resize(state.detail_area);
	}

	pub fn cancel(&self) {
		let Item::Build(build) = self.tree.get_selected() else {
			return;
		};
		let client = self.handle.clone();
		tokio::spawn(async move { build.cancel(&client).await.ok() });
	}

	pub fn quit(&self) {
		self.stop();
	}

	pub fn up(&self) {
		let mut state = self.state.write().unwrap();
		if state.show_detail {
			state.detail.up();
		} else {
			self.tree.up();
			state.detail = Detail::new(&self.handle, &self.tree.get_selected(), state.detail_area);
		}
	}

	pub fn down(&self) {
		let mut state = self.state.write().unwrap();
		if state.show_detail {
			state.detail.down();
		} else {
			self.tree.down();
			state.detail = Detail::new(&self.handle, &self.tree.get_selected(), state.detail_area);
		}
	}

	pub fn expand_children(&self) {
		if let Item::Build(_) = self.tree.get_selected() {
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
		let Ok(mut ctx) = copypasta::ClipboardContext::new() else {
			return;
		};
		let selected = self.tree.get_selected();
		let text = match selected {
			Item::Root => return,
			Item::Build(build) => build.id().to_string(),
			Item::Value { value, .. } => value.to_string(),
			Item::Package { dependency, .. } => dependency.to_string(),
		};
		ctx.set_contents(text).ok();
	}

	pub fn push(&self) {
		self.tree.push();
	}

	pub fn pop(&self) {
		self.tree.pop();
	}

	pub fn render(&self, rect: tui::layout::Rect, buf: &mut tui::buffer::Buffer) {
		let state = self.state.read().unwrap();
		if state.show_help {
			let area = render_block_and_get_area("Help", true, rect, buf);
			self.commands.render_full(area, buf);
			return;
		}

		let layout = Layout::default()
			.direction(Direction::Vertical)
			.constraints([Constraint::Fill(1), Constraint::Max(1)]);
		let rects = layout.split(rect);
		let (view_area, help_area) = (rects[0], rects[1]);
		self.commands.render_short(help_area, buf);
		if state.split {
			let layout = Layout::default()
				.direction(state.direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)]);
			let rects = layout.split(view_area);
			let (tree_area, detail_area) = (rects[0], rects[1]);

			let tree_area = render_block_and_get_area("Tree", !state.show_detail, tree_area, buf);
			self.tree.render(tree_area, buf);

			let detail_area =
				render_block_and_get_area("Detail", state.show_detail, detail_area, buf);
			state.detail.render(detail_area, buf);
		} else if state.show_detail {
			let view_area = render_block_and_get_area("Detail", true, view_area, buf);
			state.detail.render(view_area, buf);
		} else {
			let view_area = render_block_and_get_area("Tree", true, view_area, buf);
			self.tree.render(view_area, buf);
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
