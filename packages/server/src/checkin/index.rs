use {
	crate::{
		Session,
		checkin::{Graph, IndexCheckoutArgs, IndexObjectArgs, Permissions},
	},
	indexmap::IndexMap,
	num::ToPrimitive as _,
	std::{collections::VecDeque, path::Path},
	tangram_client::prelude::*,
	tangram_index::Index as _,
};

pub(super) struct CheckinIndexArg<'a> {
	pub arg: &'a tg::checkin::Arg,
	pub graph: &'a Graph,
	pub index_checkout_args: IndexCheckoutArgs,
	pub index_object_args: IndexObjectArgs,
	pub permissions: &'a Permissions,
	pub root: &'a Path,
	pub touched_at: i64,
}

impl Session {
	pub(super) fn checkin_index(
		&self,
		arg: CheckinIndexArg<'_>,
	) -> tg::Result<tangram_index::batch::Arg> {
		let CheckinIndexArg {
			arg,
			graph,
			index_checkout_args,
			index_object_args,
			permissions,
			root,
			touched_at,
		} = arg;
		// Create put checkout args.
		let mut put_index_checkout_args = Vec::new();
		if arg.options.checkout_pointers {
			if arg.options.destructive {
				let index = graph.paths.get(root).unwrap();
				let dependencies = Self::checkin_get_checkout_dependencies(graph, *index);
				let id: tg::artifact::Id = graph
					.nodes
					.get(index)
					.unwrap()
					.id
					.as_ref()
					.unwrap()
					.clone()
					.try_into()
					.unwrap();
				put_index_checkout_args.push(tangram_index::checkout::put::Arg {
					dependencies,
					id: id.into(),
					touched_at,
				});
			} else {
				// Add checkout args.
				for arg in index_checkout_args {
					put_index_checkout_args.push(tangram_index::checkout::put::Arg {
						dependencies: arg.dependencies,
						id: arg.id,
						touched_at: arg.touched_at,
					});
				}
			}
		}

		// Create put object args in reverse topological order.
		let put_grant_args =
			self.checkin_index_create_grants(graph, &index_object_args, permissions, touched_at)?;
		let put_index_object_args: Vec<_> = index_object_args.into_values().rev().collect();

		// Create the index batch.
		let arg = tangram_index::batch::Arg {
			items: put_index_checkout_args
				.into_iter()
				.map(tangram_index::batch::Item::PutCheckout)
				.chain(
					put_index_object_args
						.into_iter()
						.map(tangram_index::batch::Item::PutObject),
				)
				.chain(
					put_grant_args
						.into_iter()
						.map(tangram_index::batch::Item::PutGrant),
				)
				.collect(),
		};

		Ok(arg)
	}

	fn checkin_index_create_grants(
		&self,
		graph: &Graph,
		objects: &IndexObjectArgs,
		permissions: &Permissions,
		touched_at: i64,
	) -> tg::Result<Vec<tangram_index::grant::put::Arg>> {
		let subject = match &self.context.principal {
			tg::Principal::Anonymous => Some(tg::authorization::Subject::Public),
			tg::Principal::Root => None,
			principal => Some(principal.try_to_subject()?),
		};
		let Some(subject) = subject else {
			return Ok(Vec::new());
		};

		// Build a parent count for the new object graph.
		let mut parents = vec![0; objects.len()];
		for object in objects.values() {
			for child in &object.children {
				if let Some(index) = objects.get_index_of(child) {
					parents[index] += 1;
				}
			}
		}
		let mut queue = parents
			.iter()
			.enumerate()
			.filter_map(|(index, &parents)| (parents == 0).then_some(index))
			.collect::<VecDeque<_>>();

		// Emit the minimum grant frontier from parents to children.
		let expires_at = touched_at
			+ self
				.server
				.config
				.object
				.grant_time_to_live
				.as_secs()
				.to_i64()
				.unwrap();
		let mut args = Vec::new();
		let mut covered = vec![false; objects.len()];
		let mut external = IndexMap::<_, bool, tg::id::BuildHasher>::default();
		while let Some(index) = queue.pop_front() {
			let (id, object) = objects.get_index(index).unwrap();
			let mut subtree = false;
			if !covered[index] {
				let permissions = Self::checkin_object_permissions(graph, permissions, id);
				let permission =
					if permissions.contains(tg::authorization::permission::object::Set::SUBTREE) {
						subtree = true;
						tg::authorization::permission::object::Permission::Subtree
					} else {
						tg::authorization::permission::object::Permission::Node
					};
				let permissions = tg::authorization::Permission::Object(permission).into();
				let arg = tangram_index::grant::put::Arg {
					created_at: touched_at,
					creator: Some(self.context.principal.clone()),
					implicit: Some(Some(expires_at)),
					permissions,
					resource: id.clone().into(),
					subject: subject.clone(),
					time_to_touch: Some(self.server.config.object.grant_time_to_touch),
				};
				args.push(arg);
			}

			let covers_children = covered[index] || subtree;
			for child in &object.children {
				if let Some(child_index) = objects.get_index_of(child) {
					covered[child_index] |= covers_children;
					parents[child_index] -= 1;
					if parents[child_index] == 0 {
						queue.push_back(child_index);
					}
				} else {
					external
						.entry(child.clone())
						.and_modify(|covered| *covered |= covers_children)
						.or_insert(covers_children);
				}
			}
		}

		// Emit grants for uncovered external boundaries without descending into old graphs.
		for (id, _) in external.iter().filter(|(_, covered)| !**covered) {
			let permissions = Self::checkin_object_permissions(graph, permissions, id);
			let permission =
				if permissions.contains(tg::authorization::permission::object::Set::SUBTREE) {
					tg::authorization::permission::object::Permission::Subtree
				} else if permissions.contains(tg::authorization::permission::object::Set::NODE) {
					tg::authorization::permission::object::Permission::Node
				} else {
					continue;
				};
			let permissions = tg::authorization::Permission::Object(permission).into();
			let arg = tangram_index::grant::put::Arg {
				created_at: touched_at,
				creator: Some(self.context.principal.clone()),
				implicit: Some(Some(expires_at)),
				permissions,
				resource: id.clone().into(),
				subject: subject.clone(),
				time_to_touch: Some(self.server.config.object.grant_time_to_touch),
			};
			args.push(arg);
		}

		Ok(args)
	}

	pub(super) fn checkin_index_task(
		&self,
		arg: tangram_index::batch::Arg,
		checkin_arg: &tg::checkin::Arg,
		root: &Path,
	) -> tangram_futures::task::Shared<tg::Result<()>> {
		let updates = checkin_arg
			.updates
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>()
			.join(",");
		self.server.index_tasks.spawn({
			let root = root.to_owned();
			let server = self.server.clone();
			|_| async move {
				crate::checkpoint!(
					server,
					"checkin.index",
					path = %root.display(),
					updates,
				)
				.await;
				let result = server
					.index
					.batch(arg)
					.await
					.map_err(|error| tg::error!(!error, "failed to index the checkin"));
				if let Err(error) = &result {
					tracing::error!(error = %error.trace(), "failed to index the checkin");
				}

				result
			}
		})
	}
}
