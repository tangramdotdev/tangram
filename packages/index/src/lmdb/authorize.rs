use {
	crate::{
		authorize::{
			Batch,
			facts::{self, Output, Request},
		},
		lmdb::{Db, Index},
	},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

impl Index {
	pub async fn authorize_batch(
		&self,
		args: &[crate::authorize::Arg],
		config: crate::authorize::Config,
		principal: &tg::Principal,
	) -> tg::Result<Vec<crate::authorize::Outcome>> {
		let request = crate::read::Request::AuthorizeBatch {
			args: args.to_owned(),
			config,
			principal: principal.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::AuthorizeBatch(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn authorize_batch_with_transaction(
		config: crate::authorize::Config,
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<Vec<crate::authorize::Outcome>> {
		let (client, receiver) = facts::channel::<facts::LmdbError>(1);
		let authorize = Batch::authorize(args, client, config, principal);
		let provide = facts::serve(receiver, 1, |request| async move {
			Self::execute_authorization_fact_with_transaction(db, subspace, transaction, &request)
				.map(ControlFlow::Break)
		});
		let (outcome, ()) = futures::executor::block_on(futures::future::join(authorize, provide));
		let outcome = match outcome? {
			ControlFlow::Break(outcome) => outcome,
			ControlFlow::Continue(error) => match error {},
		};

		Ok(outcome)
	}

	fn execute_authorization_fact_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		request: &Request,
	) -> tg::Result<Output> {
		let output = match request {
			Request::Group { group } => {
				let group = Self::try_get_group_with_transaction(db, subspace, transaction, group)?;

				Output::Group(group)
			},
			Request::Id { id } => {
				let id = Self::try_resolve_id_with_transaction(db, subspace, transaction, id)?;

				Output::Id(id)
			},
			Request::MemberGroups { member } => {
				let groups =
					Self::get_member_groups_with_transaction(db, subspace, transaction, member)?;

				Output::MemberGroups(groups)
			},
			Request::MemberOrganizations { member } => {
				let organizations = Self::get_member_organizations_with_transaction(
					db,
					subspace,
					transaction,
					member,
				)?;

				Output::MemberOrganizations(organizations)
			},
			Request::ObjectChildren {
				after,
				limit,
				object,
			} => {
				let object = object.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(
						crate::lmdb::Kind::ObjectChild.to_i32().unwrap(),
						object.as_ref(),
					),
				);
				let (keys, after) = Self::get_authorization_key_page_with_transaction(
					db,
					subspace,
					transaction,
					&prefix,
					after.as_deref(),
					*limit,
				)?;
				let ids = keys
					.into_iter()
					.map(|key| {
						let crate::lmdb::Key::Object(crate::lmdb::object::Key::ObjectChild {
							child,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tg::Id::from(child))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::ObjectParents {
				after,
				limit,
				object,
			} => {
				let object = object.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(
						crate::lmdb::Kind::ChildObject.to_i32().unwrap(),
						object.as_ref(),
					),
				);
				let (keys, after) = Self::get_authorization_key_page_with_transaction(
					db,
					subspace,
					transaction,
					&prefix,
					after.as_deref(),
					*limit,
				)?;
				let ids = keys
					.into_iter()
					.map(|key| {
						let crate::lmdb::Key::Object(crate::lmdb::object::Key::ChildObject {
							object,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tg::Id::from(object))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::ObjectProcesses { object } => {
				let processes =
					Self::get_object_processes_with_transaction(db, subspace, transaction, object)?;

				Output::ObjectProcesses(processes)
			},
			Request::OwnerSandboxes {
				after,
				limit,
				owner,
			} => {
				let prefix = Self::pack(
					subspace,
					&(
						crate::lmdb::Kind::OwnerSandbox.to_i32().unwrap(),
						owner.to_string(),
					),
				);
				let (keys, after) = Self::get_authorization_key_page_with_transaction(
					db,
					subspace,
					transaction,
					&prefix,
					after.as_deref(),
					*limit,
				)?;
				let ids =
					keys.into_iter()
						.map(|key| {
							let crate::lmdb::Key::Sandbox(
								crate::lmdb::sandbox::Key::OwnerSandbox { sandbox, .. },
							) = key
							else {
								return Err(tg::error!("unexpected key type"));
							};

							Ok(tg::Id::from(sandbox))
						})
						.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::Process { process } => {
				let process =
					Self::try_get_process_with_transaction(db, subspace, transaction, process)?;

				Output::Process(process)
			},
			Request::ProcessChildren {
				after,
				limit,
				process,
			} => {
				let process = process.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(
						crate::lmdb::Kind::ProcessChild.to_i32().unwrap(),
						process.as_ref(),
					),
				);
				let (keys, after) = Self::get_authorization_key_page_with_transaction(
					db,
					subspace,
					transaction,
					&prefix,
					after.as_deref(),
					*limit,
				)?;
				let ids =
					keys.into_iter()
						.map(|key| {
							let crate::lmdb::Key::Process(
								crate::lmdb::process::Key::ProcessChild { child, .. },
							) = key
							else {
								return Err(tg::error!("unexpected key type"));
							};

							Ok(tg::Id::from(child))
						})
						.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::ProcessGrants {
				after,
				limit,
				process,
			} => {
				let subject = tg::authorization::Subject::Process(process.clone());
				let (after, grants) = Self::get_authorization_subject_grants_with_transaction(
					db,
					subspace,
					transaction,
					&subject,
					after.as_deref(),
					*limit,
				)?;

				Output::Grants { after, grants }
			},
			Request::ProcessObjects { process } => {
				let objects =
					Self::get_process_objects_with_transaction(db, subspace, transaction, process)?;

				Output::ProcessObjects(objects)
			},
			Request::ProcessParents {
				after,
				limit,
				process,
			} => {
				let process = process.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(
						crate::lmdb::Kind::ChildProcess.to_i32().unwrap(),
						process.as_ref(),
					),
				);
				let (keys, after) = Self::get_authorization_key_page_with_transaction(
					db,
					subspace,
					transaction,
					&prefix,
					after.as_deref(),
					*limit,
				)?;
				let ids =
					keys.into_iter()
						.map(|key| {
							let crate::lmdb::Key::Process(
								crate::lmdb::process::Key::ChildProcess { parent, .. },
							) = key
							else {
								return Err(tg::error!("unexpected key type"));
							};

							Ok(tg::Id::from(parent))
						})
						.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::ResourceGrants { resource } => {
				let grants = Self::get_resource_grants_with_transaction(
					db,
					subspace,
					transaction,
					resource,
				)?;

				Output::Grants {
					after: None,
					grants,
				}
			},
			Request::SandboxOwner { sandbox } => {
				let owner =
					Self::try_get_sandbox_with_transaction(db, subspace, transaction, sandbox)?
						.and_then(|sandbox| sandbox.data)
						.and_then(|data| data.data.owner);

				Output::SandboxOwner(owner)
			},
			Request::SandboxProcesses {
				after,
				limit,
				sandbox,
			} => {
				let sandbox = sandbox.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(
						crate::lmdb::Kind::SandboxProcess.to_i32().unwrap(),
						sandbox.as_ref(),
					),
				);
				let (keys, after) = Self::get_authorization_key_page_with_transaction(
					db,
					subspace,
					transaction,
					&prefix,
					after.as_deref(),
					*limit,
				)?;
				let ids =
					keys.into_iter()
						.map(|key| {
							let crate::lmdb::Key::Sandbox(
								crate::lmdb::sandbox::Key::SandboxProcess { process, .. },
							) = key
							else {
								return Err(tg::error!("unexpected key type"));
							};

							Ok(tg::Id::from(process))
						})
						.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::Specifier { specifier } => {
				let id = Self::try_get_node_with_transaction(db, subspace, transaction, specifier)?;

				Output::Id(id)
			},
			Request::SubjectGrants {
				after,
				limit,
				subject,
			} => {
				let (after, grants) = Self::get_authorization_subject_grants_with_transaction(
					db,
					subspace,
					transaction,
					subject,
					after.as_deref(),
					*limit,
				)?;

				Output::Grants { after, grants }
			},
			Request::Tag { tag } => {
				let tag = Self::try_get_tag_with_transaction(db, subspace, transaction, tag)?;

				Output::Tag(tag)
			},
			Request::TargetTags { target } => {
				let target = target.to_bytes();
				let tags = Self::get_target_tags_with_transaction(
					db,
					subspace,
					transaction,
					target.as_ref(),
				)?;

				Output::Tags(tags)
			},
		};

		Ok(output)
	}

	fn get_authorization_key_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<(Vec<crate::lmdb::Key>, Option<Vec<u8>>)> {
		let (entries, after) = Self::get_authorization_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			prefix,
			after,
			limit,
		)?;
		let keys = entries.into_iter().map(|(key, _)| key).collect();

		Ok((keys, after))
	}

	fn get_authorization_entry_page_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<(Vec<(crate::lmdb::Key, Vec<u8>)>, Option<Vec<u8>>)> {
		let start = if let Some(after) = after {
			let mut start = after.to_vec();
			start.push(0);
			start
		} else {
			prefix.to_vec()
		};
		let range = (
			std::ops::Bound::Included(start.as_slice()),
			std::ops::Bound::Unbounded,
		);
		let iter = db
			.range(transaction, &range)
			.map_err(|error| tg::error!(!error, "failed to page authorization relationships"))?;
		let mut entries = Vec::new();
		let mut last = None;
		for entry in iter.take(limit) {
			let (key, value) = entry.map_err(|error| {
				tg::error!(!error, "failed to read an authorization relationship")
			})?;
			if !key.starts_with(prefix) {
				break;
			}
			last = Some(key.to_vec());
			let key = Self::unpack(subspace, key)?;
			entries.push((key, value.to_vec()));
		}
		let after = (entries.len() == limit).then_some(last).flatten();

		Ok((entries, after))
	}

	fn get_authorization_subject_grants_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		subject: &tg::authorization::Subject,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<(Option<Vec<u8>>, Vec<crate::grant::Fact>)> {
		let prefix = Self::pack(
			subspace,
			&(
				crate::lmdb::Kind::SubjectGrant.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let (entries, after) = Self::get_authorization_entry_page_with_transaction(
			db,
			subspace,
			transaction,
			&prefix,
			after,
			limit,
		)?;
		let grants = entries
			.into_iter()
			.map(|(key, value)| {
				let crate::lmdb::Key::Grant(crate::lmdb::grant::Key::SubjectGrant {
					creator,
					permission,
					resource,
					subject,
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::lmdb::grant::GrantValue::deserialize(&value)?;
				let grant = crate::grant::Fact {
					creator,
					implicit: value.implicit.is_some(),
					permission,
					resource,
					subject,
				};

				Ok(grant)
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok((after, grants))
	}
}
