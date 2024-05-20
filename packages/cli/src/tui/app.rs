use super::{commands::Commands, detail::Detail, tree::Tree, util::render_block_and_get_area};
use copypasta::ClipboardProvider;
use crossterm::event::{Event, KeyEvent, MouseEvent, MouseEventKind};
use either::Either;
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
	pub fn new(
		handle: &H,
		object: Either<tg::Build, tg::Value>,
		rect: tui::layout::Rect,
	) -> Arc<Self> {
		let handle = handle.clone();
		let layout = tui::layout::Layout::default()
			.direction(Direction::Vertical)
			.margin(0)
			.constraints([Constraint::Max(1), Constraint::Fill(1), Constraint::Max(3)]);
		let layouts = layout.split(rect);
		let commands = Commands::new();
		let detail = Detail::new(&handle, object.clone(), layouts[1]);
		let tree = Tree::new(&handle, &[object], layouts[0]);
		let stop = AtomicBool::new(false);

		let state = RwLock::new(State {
			detail,
			detail_area: layouts[0],
			show_help: false,
			show_detail: false,
			split: false,
			direction: Direction::Horizontal,
		});

		Arc::new(Self {
			handle,
			commands,
			tree,
			state,
			stop,
		})
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
		let view_area = rects[1];
		if state.split {
			let layout = Layout::default()
				.direction(state.direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)]);
			let rects = layout.split(view_area);
			let (tree_area, detail_area) = (rects[0], rects[1]);
			self.tree.resize(tree_area);
			state.detail_area = detail_area;
		} else {
			let layout = Layout::default()
				.direction(Direction::Vertical)
				.constraints([Constraint::Max(1), Constraint::Fill(1)]);
			let rects = layout.split(view_area);
			let view_area = rects[0];
			self.tree.resize(view_area);
			state.detail_area = view_area;
		}
		state.detail.resize(state.detail_area);
	}

	pub fn cancel(&self) {
		let Either::Left(build) = self.tree.get_selected() else {
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
			state.detail = Detail::new(&self.handle, self.tree.get_selected(), state.detail_area);
		}
	}

	pub fn down(&self) {
		let mut state = self.state.write().unwrap();
		if state.show_detail {
			state.detail.down();
		} else {
			self.tree.down();
			state.detail = Detail::new(&self.handle, self.tree.get_selected(), state.detail_area);
		}
	}

	pub fn expand_objects(&self) {
		self.tree.expand_object_children();
	}

	pub fn collapse_objects(&self) {
		self.tree.collapse_object_children();
	}

	pub fn expand_children(&self) {
		match self.tree.get_selected() {
			Either::Left(_) => self.tree.expand_build_children(),
			Either::Right(_) => self.tree.expand_object_children(),
		}
	}

	pub fn collapse_children(&self) {
		self.tree.collapse_build_children();
	}

	pub fn copy_selected_to_clipboard(&self) {
		let Ok(mut ctx) = copypasta::ClipboardContext::new() else {
			return;
		};
		let selected = self.tree.get_selected();
		match selected {
			Either::Left(build) => {
				ctx.set_contents(build.id().to_string()).ok();
			},
			Either::Right(value) => {
				ctx.set_contents(value.to_string()).ok();
			},
		}
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
			let area = render_block_and_get_area("Help", rect, buf);
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

			let tree_area = render_block_and_get_area("Tree", tree_area, buf);
			self.tree.render(tree_area, buf);

			let detail_area = render_block_and_get_area("Detail", detail_area, buf);
			state.detail.render(detail_area, buf);
		} else if state.show_detail {
			let view_area = render_block_and_get_area("Detail", view_area, buf);
			state.detail.render(view_area, buf);
		} else {
			let view_area = render_block_and_get_area("Tree", view_area, buf);
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
