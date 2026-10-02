use {
	super::{Arg, Config, Sync as Candidate, engine::Batch, facts},
	std::{
		collections::{BTreeMap, BTreeSet, VecDeque},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
};

pub(super) struct Output {
	pub exhausted: bool,
	pub syncs: Vec<Candidate>,
}

pub(super) async fn discover<E>(
	arg: &Arg,
	permissions: tg::authorization::permission::Set,
	client: &facts::Client<E>,
	config: Config,
	principal: &tg::Principal,
) -> Result<ControlFlow<Output, E>, tg::Error>
where
	E: Clone + Send + Sync + 'static,
{
	let client = client.with_budget(config.permissions.ancestor.max_edges);
	let mut syncs = BTreeSet::new();
	let resource = match &arg.resource {
		tg::Selector::Id(id) => id.clone(),
		tg::Selector::Specifier(specifier) => {
			let request = facts::Request::Specifier {
				specifier: specifier.clone(),
			};
			let output = match client.read(request).await {
				Ok(ControlFlow::Break(output)) => output,
				Ok(ControlFlow::Continue(error)) => return Ok(ControlFlow::Continue(error)),
				Err(error) if facts::Client::<E>::is_budget_exhausted(&error) => {
					return Ok(ControlFlow::Break(Output {
						exhausted: true,
						syncs: Vec::new(),
					}));
				},
				Err(error) => return Err(error),
			};
			let Some(id) = output.into_id()? else {
				return Ok(ControlFlow::Break(Output {
					exhausted: false,
					syncs: Vec::new(),
				}));
			};
			id
		},
	};
	let roots = permissions
		.iter()
		.map(|permission| (resource.clone(), permission, arg.subject.clone()));
	let mut pending = roots
		.map(|key| (key.clone(), key.0.clone(), key.2.clone(), 0))
		.collect::<VecDeque<_>>();
	let mut seen = BTreeSet::new();
	let mut access = BTreeMap::new();
	let mut edges = 0;

	macro_rules! read {
		($request:expr) => {
			match client.read($request).await {
				Ok(ControlFlow::Break(output)) => output,
				Ok(ControlFlow::Continue(error)) => return Ok(ControlFlow::Continue(error)),
				Err(error) if facts::Client::<E>::is_budget_exhausted(&error) => {
					return Ok(ControlFlow::Break(Output {
						exhausted: true,
						syncs: syncs.into_iter().collect(),
					}))
				},
				Err(error) => return Err(error),
			}
		};
	}
	while let Some((origin, resource, scope, depth)) = pending.pop_front() {
		let key = (origin.clone(), resource.clone(), scope.clone());
		if seen.contains(&key) {
			continue;
		}
		if depth > config.permissions.ancestor.max_depth
			|| seen.len() >= config.permissions.ancestor.max_nodes
			|| edges >= config.permissions.ancestor.max_edges
		{
			return Ok(ControlFlow::Break(Output {
				exhausted: true,
				syncs: syncs.into_iter().collect(),
			}));
		}
		seen.insert(key);
		if let Some(tg::authorization::Subject::Sync(sync)) = &scope {
			syncs.insert(Candidate {
				permission: origin.1,
				resource: origin.0.clone(),
				sync: sync.clone(),
			});
			continue;
		}

		// Discover sync permissions already indexed on the requested node.
		if resource == origin.0 {
			let mut after = None;
			loop {
				let request = facts::Request::ResourcePermissions {
					after,
					limit: config.permissions.ancestor.page_size,
					resource: resource.clone(),
				};
				let output = read!(request);
				let (next, permissions) = output.into_permissions()?;
				for permission in permissions {
					if !permission.permission.implies(origin.1) {
						continue;
					}
					if let tg::authorization::Subject::Sync(sync) = permission.subject {
						let read = tg::authorization::Permission::Sync(
							tg::authorization::permission::sync::Permission::Read,
						);
						let key = (tg::Id::from(sync.clone()), read, scope.clone());
						let allowed =
							match permits(arg, &client, config, principal, &mut access, key).await?
							{
								ControlFlow::Break(Some(allowed)) => allowed,
								ControlFlow::Break(None) => {
									return Ok(ControlFlow::Break(Output {
										exhausted: true,
										syncs: syncs.into_iter().collect(),
									}));
								},
								ControlFlow::Continue(error) => {
									return Ok(ControlFlow::Continue(error));
								},
							};
						if allowed {
							syncs.insert(Candidate {
								permission: origin.1,
								resource: origin.0.clone(),
								sync,
							});
						}
					}
				}
				if next.is_none() {
					break;
				}
				after = next;
			}
		}

		// Follow delegations only inside their reachable roots and verified recipient contexts.
		let mut after = None;
		loop {
			let request = facts::Request::Delegations {
				after,
				limit: config.permissions.ancestor.page_size,
				resource: resource.clone(),
			};
			let output = read!(request);
			let facts::Output::Delegations {
				after: next,
				delegations,
			} = output
			else {
				return Err(tg::error!("expected delegation facts"));
			};
			for delegation in delegations {
				let subject = scope.clone().or_else(|| principal.try_to_subject().ok());
				let direct = delegation.subject == tg::authorization::Subject::Public
					|| subject.as_ref() == Some(&delegation.subject)
					|| (scope.is_none() && matches!(principal, tg::Principal::Root));
				let requirement = match &delegation.subject {
					tg::authorization::Subject::Process(process) => Some((
						process.clone().into(),
						tg::authorization::Permission::Process(
							tg::authorization::permission::process::Permission::Parent,
						),
						scope.clone(),
					)),
					tg::authorization::Subject::Tag(tag) => Some((
						tag.clone().into(),
						tg::authorization::Permission::Tag(
							tg::authorization::permission::tag::Permission::Read,
						),
						scope.clone(),
					)),
					_ => None,
				};
				let allowed = if direct {
					true
				} else if let Some(key) = requirement {
					match permits(arg, &client, config, principal, &mut access, key).await? {
						ControlFlow::Break(Some(allowed)) => allowed,
						ControlFlow::Break(None) => {
							return Ok(ControlFlow::Break(Output {
								exhausted: true,
								syncs: syncs.into_iter().collect(),
							}));
						},
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					}
				} else {
					false
				};
				if allowed {
					edges += 1;
					if edges <= config.permissions.ancestor.max_edges {
						if let tg::authorization::Subject::Sync(sync) = delegation.source {
							syncs.insert(Candidate {
								permission: origin.1,
								resource: origin.0.clone(),
								sync,
							});
						} else {
							pending.push_back((
								origin.clone(),
								origin.0.clone(),
								Some(delegation.source),
								depth + 1,
							));
						}
					}
				}
			}
			if next.is_none() {
				break;
			}
			after = next;
		}

		let is_object = tg::object::Id::try_from(resource.clone()).is_ok();
		let is_process = resource.kind() == tg::id::Kind::Process;
		if !is_object && !is_process {
			continue;
		}
		let mut after = None;
		loop {
			let request = if is_object {
				facts::Request::ObjectParents {
					after,
					limit: config.permissions.ancestor.page_size,
					object: resource.clone().try_into()?,
				}
			} else {
				facts::Request::ProcessParents {
					after,
					limit: config.permissions.ancestor.page_size,
					process: resource.clone().try_into()?,
				}
			};
			let output = read!(request);
			let (next, parents) = output.into_ids()?;
			for parent in parents {
				edges += 1;
				if edges <= config.permissions.ancestor.max_edges {
					pending.push_back((origin.clone(), parent, scope.clone(), depth + 1));
				}
			}
			if next.is_none() {
				break;
			}
			after = next;
		}
		if is_object {
			let mut after = None;
			loop {
				let request = facts::Request::ObjectProcesses {
					after,
					limit: config.permissions.ancestor.page_size,
					object: resource.clone().try_into()?,
				};
				let output = read!(request);
				let (next, processes) = output.into_object_processes()?;
				for (process, kind) in processes {
					let process_scope = Some(tg::authorization::Subject::Process(process.clone()));
					if process_scope != scope {
						let permission = tg::authorization::Permission::Process(
							super::process_object_permission(kind),
						);
						let key = (tg::Id::from(process.clone()), permission, scope.clone());
						let allowed =
							match permits(arg, &client, config, principal, &mut access, key).await?
							{
								ControlFlow::Break(Some(allowed)) => allowed,
								ControlFlow::Break(None) => {
									return Ok(ControlFlow::Break(Output {
										exhausted: true,
										syncs: syncs.into_iter().collect(),
									}));
								},
								ControlFlow::Continue(error) => {
									return Ok(ControlFlow::Continue(error));
								},
							};
						if allowed {
							edges += 1;
							pending.push_back((
								origin.clone(),
								origin.0.clone(),
								process_scope,
								depth + 1,
							));
						}
					}
					edges += 1;
					if edges <= config.permissions.ancestor.max_edges {
						pending.push_back((
							origin.clone(),
							process.into(),
							scope.clone(),
							depth + 1,
						));
					}
				}
				if next.is_none() {
					break;
				}
				after = next;
			}
		}

		// Subtree storage can depend on syncs attached to individual descendants.
		if resource != origin.0 {
			continue;
		}
		let subtree = origin.1 == origin.1.subtree();
		let mut requests = Vec::new();
		if is_object && subtree {
			requests.push(facts::Request::ObjectChildren {
				after: None,
				limit: config.permissions.ancestor.page_size,
				object: resource.clone().try_into()?,
			});
		} else if is_process {
			if subtree {
				requests.push(facts::Request::ProcessChildren {
					after: None,
					limit: config.permissions.ancestor.page_size,
					process: resource.clone().try_into()?,
				});
			}
			if origin.1
				!= tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Node,
				) && origin.1
				!= tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Subtree,
				) {
				requests.push(facts::Request::ProcessObjects {
					after: None,
					limit: config.permissions.ancestor.page_size,
					process: resource.clone().try_into()?,
				});
			}
		}
		for mut request in requests {
			loop {
				let output = read!(request.clone());
				let (next, children) = match output {
					facts::Output::ProcessObjects { after, objects } => {
						let objects = objects
							.into_iter()
							.filter(|(_, kind)| {
								origin.1.implies(tg::authorization::Permission::Process(
									super::process_object_permission(*kind),
								))
							})
							.map(|(object, _)| {
								(
									object.into(),
									tg::authorization::Permission::Object(
										tg::authorization::permission::object::Permission::Subtree,
									),
								)
							})
							.collect::<Vec<_>>();
						(after, objects)
					},
					output => {
						let (after, ids) = output.into_ids()?;
						(after, ids.into_iter().map(|id| (id, origin.1)).collect())
					},
				};
				for (child, permission) in children {
					edges += 1;
					if edges <= config.permissions.ancestor.max_edges {
						let key = (child.clone(), permission, scope.clone());
						pending.push_back((key, child, scope.clone(), depth + 1));
					}
				}
				let Some(after) = next else {
					break;
				};
				match &mut request {
					facts::Request::ObjectChildren { after: cursor, .. }
					| facts::Request::ProcessChildren { after: cursor, .. }
					| facts::Request::ProcessObjects { after: cursor, .. } => *cursor = Some(after),
					_ => unreachable!(),
				}
			}
		}
	}
	Ok(ControlFlow::Break(Output {
		exhausted: false,
		syncs: syncs.into_iter().collect(),
	}))
}

async fn permits<E>(
	arg: &Arg,
	client: &facts::Client<E>,
	config: Config,
	principal: &tg::Principal,
	access: &mut BTreeMap<super::search::Key, bool>,
	key: super::search::Key,
) -> Result<ControlFlow<Option<bool>, E>, tg::Error>
where
	E: Clone + Send + Sync + 'static,
{
	if let Some(allowed) = access.get(&key) {
		return Ok(ControlFlow::Break(Some(*allowed)));
	}
	let permissions = tg::authorization::permission::Set::from_permission(key.1);
	let check = Arg {
		requested: permissions,
		required: permissions,
		resource: tg::Selector::Id(key.0.clone()),
		storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
		subject: key.2.clone(),
		tokens: if key.2.is_none() {
			arg.tokens.clone()
		} else {
			Vec::new()
		},
	};
	let results = match Batch::verify_inner(&[check], client.clone(), config, principal).await {
		Ok(ControlFlow::Break(results)) => results,
		Ok(ControlFlow::Continue(error)) => return Ok(ControlFlow::Continue(error)),
		Err(error) if facts::Client::<E>::is_budget_exhausted(&error) => {
			return Ok(ControlFlow::Break(None));
		},
		Err(error) => return Err(error),
	};
	if matches!(results[0].outcome, super::Outcome::Exhausted) {
		return Ok(ControlFlow::Break(None));
	}
	let allowed = matches!(results[0].outcome, super::Outcome::Satisfied);
	access.insert(key, allowed);
	Ok(ControlFlow::Break(Some(allowed)))
}

#[cfg(test)]
mod tests {
	use {
		super::*,
		std::sync::{
			Arc,
			atomic::{AtomicUsize, Ordering},
		},
	};

	#[derive(Clone, Copy)]
	enum Kind {
		Candidates,
		Delegations,
		Permissions,
		Subjects,
	}

	#[tokio::test]
	async fn exhaustion_preserves_discovered_candidates() {
		bounded(Kind::Candidates).await;
	}

	#[tokio::test]
	async fn irrelevant_delegations_are_bounded() {
		bounded(Kind::Delegations).await;
	}

	#[tokio::test]
	async fn irrelevant_permissions_are_bounded() {
		bounded(Kind::Permissions).await;
	}

	#[tokio::test]
	async fn nested_subject_searches_share_the_discovery_budget() {
		bounded(Kind::Subjects).await;
	}

	async fn bounded(kind: Kind) {
		let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![200].into());
		let resource = tg::Id::from(object);
		let node = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		);
		let sync = tg::sync::Id::new();
		let source = tg::authorization::Subject::Sync(sync);
		let user = tg::authorization::Subject::User(tg::user::Id::new());
		let permissions = tg::authorization::permission::Set::from_permission(node);
		let arg = Arg {
			requested: permissions.empty_like(),
			required: permissions.empty_like(),
			resource: tg::Selector::Id(resource.clone()),
			storage: tg::storage::Set::Object(tg::object::storage::Set::NODE),
			subject: None,
			tokens: Vec::new(),
		};
		let config = Config {
			permissions: super::super::PermissionsConfig {
				ancestor: super::super::SearchConfig {
					max_edges: 40,
					page_size: 4,
					..super::super::PermissionsConfig::default().ancestor
				},
				..super::super::PermissionsConfig::default()
			},
		};
		let work = Arc::new(AtomicUsize::new(0));
		let (client, receiver) = facts::channel::<std::convert::Infallible>(2);
		let search = async move {
			let output = if matches!(kind, Kind::Candidates) {
				let results =
					Batch::verify(&[arg], client.clone(), config, &tg::Principal::Anonymous)
						.await
						.unwrap();
				let ControlFlow::Break(mut results) = results;
				let result = results.pop().unwrap();
				Output {
					exhausted: matches!(result.outcome, super::super::Outcome::Exhausted),
					syncs: result.syncs,
				}
			} else {
				let ControlFlow::Break(output) = discover(
					&arg,
					permissions,
					&client,
					config,
					&tg::Principal::Anonymous,
				)
				.await
				.unwrap();
				output
			};
			drop(client);
			output
		};
		let serve = facts::serve(receiver, 2, {
			let work = work.clone();
			let resource = resource.clone();
			move |request| {
				let work = work.clone();
				let resource = resource.clone();
				let source = source.clone();
				let user = user.clone();
				async move {
					let (output, facts) = match request {
						facts::Request::Delegations {
							after,
							limit,
							resource: requested,
						} if !matches!(kind, Kind::Permissions) && requested == resource => {
							let first_page = after.is_none();
							let next = after
								.unwrap_or_default()
								.first()
								.copied()
								.unwrap_or(0)
								.saturating_add(1);
							let delegations = (0..limit)
								.enumerate()
								.map(|(row, _)| crate::delegation::put::Arg {
									expires_at: 100,
									resource: resource.clone(),
									source: source.clone(),
									subject: if matches!(kind, Kind::Candidates)
										&& first_page && row == 0
									{
										tg::authorization::Subject::Public
									} else if matches!(kind, Kind::Subjects) {
										tg::authorization::Subject::Tag(tg::tag::Id::new())
									} else {
										user.clone()
									},
									version: None,
								})
								.collect();
							(
								facts::Output::Delegations {
									after: Some(vec![next]),
									delegations,
								},
								limit,
							)
						},
						facts::Request::ResourcePermissions {
							after,
							limit,
							resource: requested,
						} if matches!(kind, Kind::Permissions) && requested == resource => {
							let next = after
								.unwrap_or_default()
								.first()
								.copied()
								.unwrap_or(0)
								.saturating_add(1);
							let permissions = (0..limit)
								.map(|_| crate::permission::Fact {
									creator: None,
									direct: true,
									permission: node,
									resource: resource.clone(),
									subject: user.clone(),
								})
								.collect();
							(
								facts::Output::Permissions {
									after: Some(vec![next]),
									permissions,
								},
								limit,
							)
						},
						facts::Request::Delegations { .. } => (
							facts::Output::Delegations {
								after: None,
								delegations: Vec::new(),
							},
							0,
						),
						facts::Request::ResourcePermissions { .. }
						| facts::Request::SubjectPermissions { .. } => (
							facts::Output::Permissions {
								after: None,
								permissions: Vec::new(),
							},
							0,
						),
						facts::Request::Id { id } => (facts::Output::Id(Some(id)), 1),
						facts::Request::Storage { .. } => (
							facts::Output::Storage(tg::storage::Set::Object(
								tg::object::storage::Set::empty(),
							)),
							1,
						),
						facts::Request::Tag { .. } => (facts::Output::Tag(None), 1),
						facts::Request::TargetTags { .. } => (
							facts::Output::Tags {
								after: None,
								tags: Vec::new(),
							},
							0,
						),
						request => panic!("unexpected discovery fact request: {request:?}"),
					};
					work.fetch_add(1 + facts, Ordering::Relaxed);
					Ok(ControlFlow::Break(output))
				}
			}
		});
		let (output, ()) = futures::future::join(search, serve).await;
		assert!(output.exhausted);
		if matches!(kind, Kind::Candidates) {
			assert_eq!(output.syncs.len(), 1);
		}
		let resolution_work = if matches!(kind, Kind::Candidates) {
			4
		} else {
			0
		};
		assert!(
			work.load(Ordering::Relaxed) <= config.permissions.ancestor.max_edges + resolution_work
		);
	}
}
