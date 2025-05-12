use crate::Server;
use tangram_client as tg;
use tangram_either::Either;

use super::{FileDependency, State};

impl Server {
	pub(super) fn checkin_resolve_root_modules(state: &mut State) {
		// Iterate all files in the graph to update their dependencies.
		for referrer in 0..state.graph.nodes.len() {
			let Ok(file) = state.graph.nodes[referrer].variant.try_unwrap_file_ref() else {
				continue;
			};

			// Collect the dependencies that need to be updated.
			let fixups = file
				.dependencies
				.iter()
				.enumerate()
				.filter_map(|(dep_index, dependency)| match dependency {
					FileDependency::Import {
						node: Some(node), ..
					} => {
						let tag = state.graph.nodes[*node].tag.clone();
						let (path, referent) = state.graph.nodes[*node].root_module()?;
						let path = state.graph.referent_path(referrer, referent).or(Some(path));
						Some((dep_index, referent, path, tag))
					},
					FileDependency::Referent {
						referent:
							tg::Referent {
								item: Some(Either::Right(node)),
								..
							},
						..
					} => {
						let tag = state.graph.nodes[*node].tag.clone();
						let (path, referent) = state.graph.nodes[*node].root_module()?;
						let path = state.graph.referent_path(referrer, referent).or(Some(path));
						Some((dep_index, referent, path, tag))
					},
					_ => None,
				})
				.collect::<Vec<_>>();

			for (dependency, root_module, path_, tag_) in fixups {
				match &mut state.graph.nodes[referrer]
					.variant
					.unwrap_file_mut()
					.dependencies[dependency]
				{
					FileDependency::Import {
						node, path, tag, ..
					} => {
						node.replace(root_module);
						*path = path_;
						*tag = tag_;
					},
					FileDependency::Referent { referent, .. } => {
						referent.item.replace(Either::Right(root_module));
						referent.path = path_;
						referent.tag = tag_;
					},
				}
			}
		}
	}
}
