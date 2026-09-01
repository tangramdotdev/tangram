use {
	crate::{
		authorize::{
			Batch,
			facts::{self, Output, Request},
		},
		fdb::{Index, Key, Kind},
	},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	num_traits::ToPrimitive as _,
	std::{ops::ControlFlow, sync::Arc},
	tangram_client::prelude::*,
};

struct FactContext {
	subspace: Subspace,
	txn: crate::fdb::Transaction,
}

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

	pub(crate) async fn authorize_batch_with_transaction(
		concurrency: usize,
		config: crate::authorize::Config,
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		args: &[crate::authorize::Arg],
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<Vec<crate::authorize::Outcome>, fdb::FdbError>> {
		let concurrency = concurrency.max(1);
		let (client, receiver) = facts::channel(concurrency);
		let authorize = Batch::authorize(args, client, config, principal);
		let context = FactContext {
			subspace: subspace.clone(),
			txn: txn.clone(),
		};
		let context = Arc::new(context);
		let provide = facts::serve(receiver, concurrency, move |request| {
			let context = context.clone();
			async move {
				Self::execute_authorization_fact_with_transaction(
					&context.txn,
					&context.subspace,
					&request,
				)
				.await
			}
		});
		let (outcome, ()) = futures::future::join(authorize, provide).await;

		outcome
	}

	async fn execute_authorization_fact_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		request: &Request,
	) -> tg::Result<ControlFlow<Output, fdb::FdbError>> {
		let output = match request {
			Request::Group { group } => {
				let group = crate::fdb::propagate!(
					Self::try_get_group_with_transaction(txn, subspace, group).await
				);

				Output::Group(group)
			},
			Request::Id { id } => {
				let id = crate::fdb::propagate!(
					Self::try_resolve_id_with_transaction(txn, subspace, id).await
				);

				Output::Id(id)
			},
			Request::MemberGroups {
				after,
				limit,
				member,
			} => {
				let member = member.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::MemberGroup.to_i32().unwrap(), member.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let groups = keys
					.into_iter()
					.map(|key| {
						let Key::Group(crate::fdb::group::Key::MemberGroup { group, .. }) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(group)
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::MemberGroups { after, groups }
			},
			Request::MemberOrganizations {
				after,
				limit,
				member,
			} => {
				let member = member.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::MemberOrganization.to_i32().unwrap(), member.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let organizations = keys
					.into_iter()
					.map(|key| {
						let Key::Organization(crate::fdb::organization::Key::MemberOrganization {
							organization,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(organization)
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::MemberOrganizations {
					after,
					organizations,
				}
			},
			Request::ObjectChildren {
				after,
				limit,
				object,
			} => {
				let object = object.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::ObjectChild.to_i32().unwrap(), object.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let ids = keys
					.into_iter()
					.map(|key| {
						let Key::Object(crate::fdb::object::Key::ObjectChild { child, .. }) = key
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
					&(Kind::ChildObject.to_i32().unwrap(), object.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let ids = keys
					.into_iter()
					.map(|key| {
						let Key::Object(crate::fdb::object::Key::ChildObject { object, .. }) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tg::Id::from(object))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::ObjectProcesses {
				after,
				limit,
				object,
			} => {
				let object = object.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::ObjectProcess.to_i32().unwrap(), object.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let processes = keys
					.into_iter()
					.map(|key| {
						let Key::Object(crate::fdb::object::Key::ObjectProcess {
							kind,
							process,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok((process, kind))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::ObjectProcesses { after, processes }
			},
			Request::OwnerSandboxes {
				after,
				limit,
				owner,
			} => {
				let prefix = Self::pack(
					subspace,
					&(Kind::OwnerSandbox.to_i32().unwrap(), owner.to_string()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let ids = keys
					.into_iter()
					.map(|key| {
						let Key::Sandbox(crate::fdb::sandbox::Key::OwnerSandbox {
							sandbox, ..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tg::Id::from(sandbox))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::Process { process } => {
				let process = crate::fdb::propagate!(
					Self::try_get_process_with_transaction(txn, subspace, process).await
				);

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
					&(Kind::ProcessChild.to_i32().unwrap(), process.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let ids = keys
					.into_iter()
					.map(|key| {
						let Key::Process(crate::fdb::process::Key::ProcessChild { child, .. }) =
							key
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
				let (after, grants) = crate::fdb::propagate!(
					Self::get_authorization_subject_grants_with_transaction(
						txn,
						subspace,
						&subject,
						after.as_deref(),
						*limit,
					)
					.await
				);

				Output::Grants { after, grants }
			},
			Request::ProcessObjects {
				after,
				limit,
				process,
			} => {
				let process = process.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::ProcessObject.to_i32().unwrap(), process.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let objects = keys
					.into_iter()
					.map(|key| {
						let Key::Process(crate::fdb::process::Key::ProcessObject {
							kind,
							object,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok((object, kind))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::ProcessObjects { after, objects }
			},
			Request::ProcessParents {
				after,
				limit,
				process,
			} => {
				let process = process.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::ChildProcess.to_i32().unwrap(), process.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let ids = keys
					.into_iter()
					.map(|key| {
						let Key::Process(crate::fdb::process::Key::ChildProcess { parent, .. }) =
							key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tg::Id::from(parent))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::ResourceGrants {
				after,
				limit,
				resource,
			} => {
				let resource_bytes = resource.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(
						Kind::ResourceGrant.to_i32().unwrap(),
						resource_bytes.as_ref(),
					),
				);
				let (entries, after) = crate::fdb::propagate!(
					Self::get_authorization_entry_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let grants = entries
					.into_iter()
					.map(|(key, value)| {
						let Key::Grant(crate::fdb::grant::Key::ResourceGrant {
							creator,
							permission,
							subject,
							..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};
						let value = crate::fdb::grant::GrantValue::deserialize(&value)?;
						let grant = crate::grant::Fact {
							creator,
							implicit: value.implicit.is_some(),
							permission,
							resource: resource.clone(),
							subject,
						};

						Ok(grant)
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Grants { after, grants }
			},
			Request::SandboxOwner { sandbox } => {
				let owner = crate::fdb::propagate!(
					Self::try_get_sandbox_with_transaction(txn, subspace, sandbox).await
				)
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
					&(Kind::SandboxProcess.to_i32().unwrap(), sandbox.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let ids = keys
					.into_iter()
					.map(|key| {
						let Key::Sandbox(crate::fdb::sandbox::Key::SandboxProcess {
							process, ..
						}) = key
						else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tg::Id::from(process))
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Ids { after, ids }
			},
			Request::Specifier { specifier } => {
				let id = crate::fdb::propagate!(
					Self::try_get_node_with_transaction(txn, subspace, specifier).await
				);

				Output::Id(id)
			},
			Request::SubjectGrants {
				after,
				limit,
				subject,
			} => {
				let (after, grants) = crate::fdb::propagate!(
					Self::get_authorization_subject_grants_with_transaction(
						txn,
						subspace,
						subject,
						after.as_deref(),
						*limit,
					)
					.await
				);

				Output::Grants { after, grants }
			},
			Request::Tag { tag } => {
				let tag = crate::fdb::propagate!(
					Self::try_get_tag_with_transaction(txn, subspace, tag).await
				);

				Output::Tag(tag)
			},
			Request::TargetTags {
				after,
				limit,
				target,
			} => {
				let target = target.to_bytes();
				let prefix = Self::pack(
					subspace,
					&(Kind::TargetTag.to_i32().unwrap(), target.as_ref()),
				);
				let (keys, after) = crate::fdb::propagate!(
					Self::get_authorization_key_page_with_transaction(
						txn,
						subspace,
						&prefix,
						after.as_deref(),
						*limit,
					)
					.await
				);
				let tags = keys
					.into_iter()
					.map(|key| {
						let Key::Tag(crate::fdb::tag::Key::TargetTag { tag, .. }) = key else {
							return Err(tg::error!("unexpected key type"));
						};

						Ok(tag)
					})
					.collect::<tg::Result<Vec<_>>>()?;

				Output::Tags { after, tags }
			},
		};

		Ok(ControlFlow::Break(output))
	}

	async fn get_authorization_key_page_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<(Vec<crate::fdb::Key>, Option<Vec<u8>>), fdb::FdbError>> {
		let (entries, after) = crate::fdb::propagate!(
			Self::get_authorization_entry_page_with_transaction(
				txn, subspace, prefix, after, limit,
			)
			.await
		);
		let keys = entries.into_iter().map(|(key, _)| key).collect();

		Ok(ControlFlow::Break((keys, after)))
	}

	async fn get_authorization_entry_page_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		prefix: &[u8],
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<(Vec<(crate::fdb::Key, Vec<u8>)>, Option<Vec<u8>>), fdb::FdbError>>
	{
		let range_subspace = Subspace::from_bytes(prefix.to_vec());
		let mut range = fdb::RangeOption {
			limit: Some(limit),
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&range_subspace)
		};
		if let Some(after) = after {
			let mut begin = after.to_vec();
			begin.push(0);
			range.begin = fdb::KeySelector::first_greater_or_equal(begin);
		}
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let after = (entries.len() == limit)
			.then(|| entries.last().map(|entry| entry.key().to_vec()))
			.flatten();
		let entries = entries
			.iter()
			.map(|entry| {
				let key = Self::unpack(subspace, entry.key())?;
				let value = entry.value().to_vec();

				Ok((key, value))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		Ok(ControlFlow::Break((entries, after)))
	}

	async fn get_authorization_subject_grants_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		subject: &tg::authorization::Subject,
		after: Option<&[u8]>,
		limit: usize,
	) -> tg::Result<ControlFlow<(Option<Vec<u8>>, Vec<crate::grant::Fact>), fdb::FdbError>> {
		let prefix = Self::pack(
			subspace,
			&(Kind::SubjectGrant.to_i32().unwrap(), subject.to_string()),
		);
		let (entries, after) = crate::fdb::propagate!(
			Self::get_authorization_entry_page_with_transaction(
				txn, subspace, &prefix, after, limit,
			)
			.await
		);
		let grants = entries
			.into_iter()
			.map(|(key, value)| {
				let Key::Grant(crate::fdb::grant::Key::SubjectGrant {
					creator,
					permission,
					resource,
					subject,
				}) = key
				else {
					return Err(tg::error!("unexpected key type"));
				};
				let value = crate::fdb::grant::GrantValue::deserialize(&value)?;
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

		Ok(ControlFlow::Break((after, grants)))
	}
}
