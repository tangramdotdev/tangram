use {crate::Session, std::collections::BTreeSet, tangram_client::prelude::*};

impl Session {
	pub(crate) fn create_permission_capture_items(
		&self,
		resource: tg::Id,
		version: Option<String>,
		roots: impl IntoIterator<Item = tg::Referent<tg::Id>>,
		principal: tg::Principal,
		created_at: i64,
	) -> tg::Result<Vec<tangram_index::batch::Item>> {
		let roots = roots
			.into_iter()
			.map(|resource| {
				let permissions =
					Self::permission_capture_permissions(&resource.node)?.empty_like();
				Ok((resource, permissions))
			})
			.collect::<tg::Result<Vec<_>>>()?;
		self.create_permission_capture_items_with_permissions(
			resource, version, roots, principal, created_at,
		)
	}

	pub(crate) fn create_permission_capture_items_with_permissions(
		&self,
		resource: tg::Id,
		version: Option<String>,
		roots: impl IntoIterator<Item = (tg::Referent<tg::Id>, tg::authorization::permission::Set)>,
		principal: tg::Principal,
		created_at: i64,
	) -> tg::Result<Vec<tangram_index::batch::Item>> {
		let (creator, subject) = match resource.kind() {
			tg::id::Kind::Process if version.is_none() => {
				let process = tg::process::Id::try_from(resource.clone())?;
				(
					Some(tg::Principal::Process(process.clone())),
					tg::authorization::Subject::Process(process),
				)
			},
			tg::id::Kind::Tag if version.is_some() => {
				let tag = tg::tag::Id::try_from(resource.clone())?;
				(None, tg::authorization::Subject::Tag(tag))
			},
			_ => return Err(tg::error!("invalid permission capture resource or version")),
		};
		let mut items = Vec::new();
		let mut pending = Vec::new();
		for (mut resource, mut proven) in roots {
			let requested = Self::permission_capture_permissions(&resource.node)?;
			let accepted = proven;
			for permission in requested.iter() {
				if accepted.iter().any(|proof| proof.implies(permission)) {
					proven.insert(permission.into());
				}
			}
			let mut tokens = Vec::new();
			if principal.is_root() {
				proven = requested;
			} else if !proven.contains(requested) {
				// Check exact proofs first so the fast path verifies only the tokens it needs.
				let local = resource.options.tokens.local_authorization();
				let exact = local
					.iter()
					.filter(|token| token.body.resource == resource.node);
				let other = local
					.iter()
					.filter(|token| token.body.resource != resource.node);
				for token in exact.chain(other) {
					if !self.verify_token(token) {
						continue;
					}
					if token.body.resource == resource.node {
						for permission in requested.iter() {
							if token.body.authorizes(permission) {
								proven.insert(permission.into());
							}
						}
					}
					tokens.push(token.clone());
					if proven.contains(requested) {
						break;
					}
				}
			}
			if !proven.is_empty() {
				let arg = tangram_index::permission::put::Arg {
					created_at,
					creator: creator.clone(),
					permissions: proven,
					resource: resource.node.clone(),
					source: tangram_index::permission::Source::Direct { expires_at: None },
					subject: subject.clone(),
					time_to_touch: None,
					version: version.clone(),
				};
				items.push(tangram_index::batch::Item::PutPermission(arg));
			}
			if proven.contains(requested) {
				continue;
			}

			// Delegate only roots that still need capture, including authority held by pending syncs.
			let time_to_live = i64::try_from(
				self.server
					.config
					.indexer
					.permission_capture
					.delegation_time_to_live
					.as_secs(),
			)
			.map_err(|error| tg::error!(!error, "failed to convert the delegation time to live"))?;
			let expires_at = created_at
				.checked_add(time_to_live)
				.ok_or_else(|| tg::error!("the delegation expiration overflowed"))?;
			let mut sources = BTreeSet::new();
			let source = match &principal {
				tg::Principal::Anonymous => tg::authorization::Subject::Public,
				source => source.try_to_subject()?,
			};
			sources.insert(source);
			for token in &tokens {
				if let Some(sync) = self.try_get_sync_id_from_token(token) {
					sources.insert(tg::authorization::Subject::Sync(sync));
				}
			}
			for source in sources {
				if source == subject {
					continue;
				}
				let arg = tangram_index::delegation::put::Arg {
					expires_at,
					resource: resource.node.clone(),
					source,
					subject: subject.clone(),
					version: version.clone(),
				};
				items.push(tangram_index::batch::Item::PutDelegation(arg));
			}
			resource.options.tokens = tg::authorization::Tokens::with_authorization(tokens);
			pending.push(resource);
		}
		if !pending.is_empty() {
			let arg = tangram_index::permission::capture::enqueue::Arg {
				id: uuid::Uuid::now_v7().as_bytes().to_vec(),
				principal,
				resource,
				roots: pending,
				version,
			};
			items.push(tangram_index::batch::Item::EnqueuePermissionCapture(arg));
		}

		Ok(items)
	}

	pub(crate) fn permission_capture_permissions(
		resource: &tg::Id,
	) -> tg::Result<tg::authorization::permission::Set> {
		if resource.kind().is_object() {
			let mut permissions = tg::authorization::permission::object::Set::NODE;
			permissions.insert(tg::authorization::permission::object::Set::SUBTREE);
			return Ok(tg::authorization::permission::Set::Object(permissions));
		}
		if resource.kind() == tg::id::Kind::Process {
			let mut permissions = tg::authorization::permission::process::Set::all();
			permissions.remove(tg::authorization::permission::process::Set::PARENT);
			return Ok(tg::authorization::permission::Set::Process(permissions));
		}
		Err(tg::error!("invalid permission capture resource"))
	}
}
