use self::{
	node::{Indicator, Node},
	provider::Provider,
};
use super::commands::Commands;
use futures::{future::BoxFuture, stream::FuturesUnordered, StreamExt as _};
use num::ToPrimitive;
use ratatui::prelude::*;
use std::sync::{Arc, RwLock};
use tangram_client as tg;
use tangram_either::Either;

mod display;
mod node;
mod provider;

pub struct Tree<H> {
	pub(super) commands: Arc<Commands<H>>,
	state: RwLock<State<H>>,
}

struct State<H> {
	rect: Rect,
	roots: Vec<Arc<RwLock<Node<H>>>>,
	scroll: usize,
	selected: Arc<RwLock<Node<H>>>,
}

#[derive(Copy, Clone, Debug)]
pub struct Options {
	pub builds: bool,
	pub collapse_builds_on_success: bool,
	pub depth: Option<u32>,
	pub objects: bool,
}

type Method<A, R> = Box<dyn FnMut(A) -> BoxFuture<'static, R> + Send + Sync>;

impl<H> Tree<H>
where
	H: tg::Handle,
{
	/// Scroll to the bottom of the tree.
	pub fn bottom(&self) {
		let nodes = self.expanded_nodes();
		let mut state = self.state.write().unwrap();
		state.scroll = nodes
			.len()
			.saturating_sub(state.rect.height.to_usize().unwrap().saturating_sub(2));
		state.selected.write().unwrap().selected = false;
		state.selected = nodes.last().unwrap().clone();
		state.selected.write().unwrap().selected = true;
	}

	/// Collapse any children of the selected node.
	pub fn collapse_children(&self) {
		let selected = self.selected();
		if selected.read().unwrap().is_collapsed() {
			if selected.read().unwrap().is_root {
				self.pop();
				return;
			}
			let Some(parent) = selected.read().unwrap().parent().clone() else {
				return;
			};

			selected.write().unwrap().selected = false;
			parent.write().unwrap().selected = true;
			self.state.write().unwrap().selected = parent.clone();
			let expanded_nodes = self.expanded_nodes();
			let index = expanded_nodes
				.iter()
				.position(|node| Arc::ptr_eq(node, &parent))
				.unwrap();
			if index < self.state.read().unwrap().scroll {
				self.state.write().unwrap().scroll = index;
			}
		} else {
			selected.write().unwrap().collapse_build_children();
			selected.write().unwrap().collapse_object_children();
		}
	}

	/// Scroll down one item in the tree.
	pub fn down(&self) {
		self.select(true);
	}

	/// Expand children.
	pub fn expand(&self, options: Options) {
		Node::expand(&self.selected(), options);
	}

	fn expanded_nodes(&self) -> Vec<Arc<RwLock<Node<H>>>> {
		let mut nodes = Vec::new();
		let root = self.state.read().unwrap().roots.last().unwrap().clone();
		let mut stack = vec![root];

		while let Some(node) = stack.pop() {
			nodes.push(node.clone());

			let node = node.read().unwrap();

			// Add the object children.
			let object_children = node.object_children.iter().flatten().rev().cloned();
			if node.options.objects {
				stack.extend(object_children);
			}

			// Add build children.
			let build_children = node.build_children.iter().flatten().rev().cloned();
			if node.options.builds {
				stack.extend(build_children);
			}
		}

		nodes
	}

	pub fn get_selected(&self) -> Either<tg::Build, tg::Value> {
		self.selected()
			.read()
			.unwrap()
			.provider
			.item
			.clone()
			.unwrap()
	}

	pub fn hit_test(&self, x: u16, y: u16) -> bool {
		self.state.read().unwrap().rect.contains(Position { x, y })
	}

	pub fn is_finished(&self) -> bool {
		let indicator = self
			.state
			.read()
			.unwrap()
			.roots
			.last()
			.unwrap()
			.read()
			.unwrap()
			.indicator;
		indicator.map_or(true, |indicator| {
			matches!(
				indicator,
				Indicator::Canceled | Indicator::Failed | Indicator::Succeeded
			)
		})
	}

	pub fn new(handle: &H, item: Either<tg::Build, tg::Object>, options: Options) -> Self {
		let provider = match item {
			Either::Left(build) => Provider::build(handle, None, &build),
			Either::Right(object) => Provider::object(handle, None, &object),
		};
		let roots = vec![Node::new(None, provider, options)];
		let selected = roots[0].clone();
		selected.write().unwrap().selected = true;
		selected.write().unwrap().is_root = true;
		let state = State {
			rect: Rect::default(),
			scroll: 0,
			roots,
			selected,
		};
		let commands = Commands::tree();
		Self {
			commands,
			state: RwLock::new(state),
		}
	}

	pub fn pop(&self) {
		let mut state = self.state.write().unwrap();
		if state.roots.len() > 1 {
			state.roots.last().unwrap().write().unwrap().is_root = false;
			state.roots.pop();
			state.roots.last().unwrap().write().unwrap().is_root = true;
			state.roots.last().unwrap().write().unwrap().selected = true;
			state.selected.write().unwrap().selected = false;
			state.selected = state.roots.last().unwrap().clone();
		}
	}

	pub fn push(&self) {
		let mut state = self.state.write().unwrap();
		let new_root = state.selected.clone();
		if new_root.read().unwrap().is_root {
			return;
		}
		state
			.roots
			.last()
			.as_mut()
			.unwrap()
			.write()
			.unwrap()
			.is_root = false;
		state.roots.push(new_root);
		state
			.roots
			.last()
			.as_mut()
			.unwrap()
			.write()
			.unwrap()
			.is_root = true;
	}

	pub fn render(&self, area: Rect, buf: &mut Buffer) {
		// Flatten the tree.
		let nodes = self.expanded_nodes();

		// Get the nodes we care about.
		let state = self.state.read().unwrap();
		let height = area.height.to_usize().unwrap();
		let nodes = nodes.iter().skip(state.scroll).take(height);

		// Render the tree.
		for (area, node) in area.rows().zip(nodes) {
			Node::render(node, area, buf);
		}
	}

	pub fn resize(&self, area: Rect) {
		self.state.write().unwrap().rect = area;
	}

	fn select(&self, down: bool) {
		let expanded_nodes = self.expanded_nodes();

		// Calculate the index of the new selected node.
		let mut state = self.state.write().unwrap();
		let previous_selected_index = expanded_nodes
			.iter()
			.position(|item| Arc::ptr_eq(item, &state.selected))
			.unwrap();
		let new_selected_index = if down {
			(previous_selected_index + 1).min(expanded_nodes.len() - 1)
		} else {
			previous_selected_index.saturating_sub(1)
		};
		if new_selected_index == previous_selected_index {
			drop(state);
			self.pop();
			return;
		}

		// Update the nodes.
		expanded_nodes[previous_selected_index]
			.write()
			.unwrap()
			.selected = false;
		expanded_nodes[new_selected_index].write().unwrap().selected = true;
		state.selected = expanded_nodes[new_selected_index].clone();

		// Update the scroll if necessary.
		let height = state.rect.height.to_usize().unwrap().saturating_sub(2);
		if new_selected_index >= state.scroll && (new_selected_index - state.scroll) >= height {
			state.scroll += 1;
		} else if new_selected_index < state.scroll {
			state.scroll = state.scroll.saturating_sub(1);
		}
	}

	fn selected(&self) -> Arc<RwLock<Node<H>>> {
		self.state.read().unwrap().selected.clone()
	}

	pub fn stop(&self) {
		for root in &self.state.read().unwrap().roots {
			root.read().unwrap().stop();
		}
	}

	/// Scroll to the top of the tree.
	pub fn top(&self) {
		let nodes = self.expanded_nodes();
		let mut state = self.state.write().unwrap();
		state.selected.write().unwrap().selected = false;
		state.selected = nodes[0].clone();
		state.selected.write().unwrap().selected = true;
		state.scroll = 0;
	}

	/// Scroll up one item in the tree.
	pub fn up(&self) {
		self.select(false);
	}

	pub async fn wait(&self) {
		let roots = self.state.write().unwrap().roots.clone();
		roots
			.into_iter()
			.map(Node::wait)
			.collect::<FuturesUnordered<_>>()
			.collect::<()>()
			.await;
	}
}
