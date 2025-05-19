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
						let referent = state.graph.nodes[*node].root_module()?;
						Some((dep_index, referent, None, None))
					},
					FileDependency::Referent {
						referent:
							tg::Referent {
								item: Some(Either::Right(node)),
								..
							},
						..
					} => {
						let referent = state.graph.nodes[*node].root_module()?;
						let path = state.graph.package_path(referrer, referent);
						let subpath = state.graph.nodes[referent].subpath.clone();
						Some((dep_index, referent, path, subpath))
					},
					_ => None,
				})
				.collect::<Vec<_>>();

			for (dependency, root_module, path, subpath) in fixups {
				match &mut state.graph.nodes[referrer]
					.variant
					.unwrap_file_mut()
					.dependencies[dependency]
				{
					FileDependency::Import { node, .. } => {
						node.replace(root_module);
					},
					FileDependency::Referent { referent, .. } => {
						referent.item.replace(Either::Right(root_module));
						referent.path = path;
						referent.subpath = subpath;
					},
				}
			}
		}
	}
}
