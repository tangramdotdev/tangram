use super::{commands::Commands, detail::Detail, tree, util::render_block_and_get_area};
use copypasta::ClipboardProvider;
use crossterm::event::{Event, KeyEvent, MouseEvent, MouseEventKind};
use derive_more::Debug;
use ratatui::{self as tui, prelude::*};
use std::sync::{
	atomic::{AtomicBool, Ordering},
	Arc, RwLock,
};
use tangram_client as tg;
use tangram_either::Either;

pub struct App<H> {
	handle: H,
	tree: Arc<tree::Tree<H>>,
	state: RwLock<State<H>>,
	stop: AtomicBool,
}

struct State<H> {
	area: Rect,
	detail_area: Rect,
	detail: Arc<Detail<H>>,
	show_help: bool,
	split: Option<Direction>,
	focus: Focus,
}

#[derive(Clone, Copy, Debug)]
pub enum Focus {
	Tree,
	DetailOne,
	DetailTwo,
}

impl<H> App<H>
where
	H: tg::Handle,
{
	#[allow(clippy::needless_pass_by_value)]
	pub fn new(
		handle: &H,
		item: Either<tg::Build, tg::Value>,
		tree: tree::Tree<H>,
		rect: tui::layout::Rect,
	) -> Arc<Self> {
		let handle = handle.clone();
		let detail = Detail::new(&handle, item.clone(), rect);
		let stop = AtomicBool::new(false);

		let split = if rect.width >= 80 {
			Some(Direction::Horizontal)
		} else if rect.height >= 30 {
			Some(Direction::Vertical)
		} else {
			None
		};

		let focus = Focus::Tree;

		let state = RwLock::new(State {
			area: rect,
			detail,
			detail_area: rect,
			show_help: false,
			split,
			focus,
		});

		let app = Arc::new(Self {
			handle,
			tree: Arc::new(tree),
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
		let (show_help, focus) = {
			let state = self.state.read().unwrap();
			(state.show_help, state.focus)
		};
		if show_help {
			let commands = Commands::help();
			commands.dispatch(event, self);
		}
		match focus {
			Focus::Tree => self.tree.commands.dispatch(event, self),
			Focus::DetailOne | Focus::DetailTwo => {
				let detail = self.state.read().unwrap().detail.clone();
				detail.commands.dispatch(event, self);
			},
		}
	}

	pub fn mouse(&self, event: MouseEvent) {
		let state = self.state.read().unwrap();
		if state.show_help {
			return;
		}
		let detail = &state.detail;
		if self.tree.hit_test(event.column, event.row) && matches!(state.focus, Focus::Tree) {
			drop(state);
			match event.kind {
				MouseEventKind::ScrollDown => self.down(true),
				MouseEventKind::ScrollUp => self.up(true),
				_ => (),
			}
		} else if detail.hit_test(event.column, event.row) {
			match event.kind {
				MouseEventKind::ScrollDown => detail.mouse_scroll(event.column, event.row, false),
				MouseEventKind::ScrollUp => detail.mouse_scroll(event.column, event.row, true),
				_ => (),
			}
		}
	}

	pub fn toggle_split(&self) {
		let mut state = self.state.write().unwrap();
		state.split = if state.split.is_none() {
			let direction = if state.area.height >= 30 {
				Direction::Vertical
			} else {
				Direction::Horizontal
			};
			Some(direction)
		} else {
			None
		}
	}

	pub fn rotate(&self) {
		let mut state = self.state.write().unwrap();
		if let Some(direction) = &mut state.split {
			*direction = match direction {
				Direction::Vertical => Direction::Horizontal,
				Direction::Horizontal => Direction::Vertical,
			};
		}
	}

	pub fn toggle_help(&self) {
		let mut state = self.state.write().unwrap();
		state.show_help = !state.show_help;
	}

	pub fn top(&self) {
		let mut state = self.state.write().unwrap();
		match state.focus {
			Focus::Tree => {
				self.tree.top();
				let selected = self.tree.get_selected();
				state.detail = Detail::new(&self.handle, selected, state.detail_area);
			},
			Focus::DetailOne | Focus::DetailTwo => {
				state.detail.top();
			},
		}
	}

	pub fn bottom(&self) {
		let mut state = self.state.write().unwrap();
		match state.focus {
			Focus::Tree => {
				self.tree.bottom();
				let selected = self.tree.get_selected();
				state.detail = Detail::new(&self.handle, selected, state.detail_area);
			},
			Focus::DetailOne | Focus::DetailTwo => {
				state.detail.bottom();
			},
		}
	}

	pub fn up(&self, mouse: bool) {
		let mut state = self.state.write().unwrap();
		if mouse | matches!(state.focus, Focus::Tree) {
			self.tree.up();
			let selected = self.tree.get_selected();
			state.detail = Detail::new(&self.handle, selected, state.detail_area);
		} else {
			state.detail.up();
		}
	}

	pub fn down(&self, mouse: bool) {
		let mut state = self.state.write().unwrap();
		if mouse | matches!(state.focus, Focus::Tree) {
			self.tree.down();
			let selected = self.tree.get_selected();
			state.detail = Detail::new(&self.handle, selected, state.detail_area);
		} else {
			state.detail.down();
		}
	}

	pub fn enter(&self) {
		let mut state = self.state.write().unwrap();
		match state.focus {
			Focus::Tree => state.focus = Focus::DetailOne,
			Focus::DetailOne | Focus::DetailTwo => (),
		}
		state.detail.set_focus(state.focus);
	}

	pub fn back(&self) {
		let mut state = self.state.write().unwrap();
		match state.focus {
			Focus::Tree => (),
			Focus::DetailOne | Focus::DetailTwo => state.focus = Focus::Tree,
		}
		state.detail.set_focus(state.focus);
	}

	pub fn tab(&self) {
		let (focus, is_split, has_data) = {
			let state = self.state.read().unwrap();
			(state.focus, state.split.is_some(), state.detail.has_log())
		};
		#[allow(clippy::match_same_arms)]
		let focus = match (focus, is_split, has_data) {
			(Focus::Tree, true, _) => Focus::DetailOne,
			(Focus::Tree, false, _) => Focus::Tree,
			(Focus::DetailOne, _, true) => Focus::DetailTwo,
			(Focus::DetailOne, true, false) => Focus::Tree,
			(Focus::DetailOne, false, false) => Focus::DetailOne,
			(Focus::DetailTwo, false, _) => Focus::DetailOne,
			(Focus::DetailTwo, true, _) => Focus::Tree,
		};
		self.state.write().unwrap().focus = focus;
		self.state.read().unwrap().detail.set_focus(focus);
	}

	pub fn resize(&self, rect: Rect) {
		let mut state = self.state.write().unwrap();
		let layout = Layout::default()
			.direction(Direction::Vertical)
			.constraints([Constraint::Fill(1), Constraint::Max(1)]);
		let rects = layout.split(rect);
		let view_area = rects[0];

		if let Some(direction) = state.split {
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
		let Either::Left(build) = self.tree.get_selected() else {
			return;
		};
		let handle = self.handle.clone();
		tokio::spawn(async move {
			let outcome =
				tg::build::outcome::Data::Cancelation(tg::build::outcome::data::Cancelation {
					reason: Some("the build was explicitly canceled".to_owned()),
				});
			let arg = tg::build::finish::Arg {
				outcome,
				remote: None,
			};
			build.finish(&handle, arg).await.ok();
		});
	}

	pub fn quit(&self) {
		self.stop();
	}

	pub fn expand_children(&self) {
		let options = tree::Options {
			depth: Some(1),
			objects: true,
			builds: true,
			collapse_builds_on_success: false,
		};
		self.tree.expand(options);
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
			Either::Left(build) => build.id().to_string(),
			Either::Right(value) => value.to_string(),
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
		let commands = match state.focus {
			Focus::Tree => self.tree.commands.clone(),
			Focus::DetailOne | Focus::DetailTwo => state.detail.commands.clone(),
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

		if let Some(direction) = state.split {
			let layout = Layout::default()
				.direction(direction)
				.constraints([Constraint::Fill(1), Constraint::Fill(1)]);
			let rects = layout.split(view_area);
			let (tree_area, detail_area) = (rects[0], rects[1]);
			let tree_focus = matches!(state.focus, Focus::Tree);
			let tree_area = render_block_and_get_area("Tree", tree_focus, tree_area, buf);
			self.tree.render(tree_area, buf);
			state.detail.render(detail_area, buf);
		} else if matches!(state.focus, Focus::Tree) {
			let view_area = render_block_and_get_area("Tree", true, view_area, buf);
			self.tree.render(view_area, buf);
		} else {
			state.detail.render(rect, buf);
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
