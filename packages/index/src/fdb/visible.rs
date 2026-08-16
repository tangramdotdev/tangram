use {
	crate::fdb::Index,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::{collections::HashSet, ops::ControlFlow},
	tangram_client::prelude::*,
};

impl Index {
	pub async fn get_requester_subjects(
		&self,
		principal: &tg::Principal,
	) -> tg::Result<Vec<tg::authorization::Subject>> {
		let request = crate::read::Request::GetRequesterSubjects {
			principal: principal.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::GetRequesterSubjects(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn visible(
		&self,
		ids: &[tg::Id],
		principal: &tg::Principal,
	) -> tg::Result<Vec<bool>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(vec![true; ids.len()]);
		}
		let request = crate::read::Request::Visible {
			ids: ids.to_owned(),
			principal: principal.clone(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::Visible(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn visible_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::Id],
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<Vec<bool>, fdb::FdbError>> {
		if matches!(principal, tg::Principal::Root) {
			return Ok(ControlFlow::Break(vec![true; ids.len()]));
		}
		let subjects = crate::fdb::propagate!(
			Self::requester_subjects_with_transaction(txn, subspace, principal).await
		);
		let output = {
			let results = futures::future::try_join_all(ids.iter().cloned().map(|id| {
				let subjects = &subjects;
				async move {
					let mut visible = false;
					for subject in subjects {
						if crate::fdb::propagate!(
							Self::try_get_visibility_with_transaction(txn, subspace, &id, subject,)
								.await
						) {
							visible = true;
							break;
						}
					}

					Ok::<_, tg::Error>(ControlFlow::Break(visible))
				}
			}))
			.await?;
			let mut output = Vec::with_capacity(results.len());
			for result in results {
				let visible = match result {
					ControlFlow::Break(visible) => visible,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				output.push(visible);
			}

			output
		};

		Ok(ControlFlow::Break(output))
	}

	pub(crate) async fn requester_subjects_with_transaction(
		txn: &crate::fdb::Transaction,
		subspace: &Subspace,
		principal: &tg::Principal,
	) -> tg::Result<ControlFlow<Vec<tg::authorization::Subject>, fdb::FdbError>> {
		let mut subjects = vec![tg::authorization::Subject::Public];
		if !matches!(principal, tg::Principal::Anonymous) {
			subjects.push(principal.try_to_subject()?);
		}
		let id = match principal {
			tg::Principal::Group(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Organization(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::User(id) => Some(tg::Id::from(id.clone())),
			tg::Principal::Anonymous
			| tg::Principal::Process(_)
			| tg::Principal::Root
			| tg::Principal::Runner(_)
			| tg::Principal::Sandbox(_) => None,
		};
		if let Some(id) = id {
			let mut frontier = vec![id.clone()];
			let mut visited = HashSet::from([id]);
			while !frontier.is_empty() {
				let relations = {
					let results =
						futures::future::try_join_all(frontier.into_iter().map(|id| async move {
							Self::get_member_groups_and_organizations_with_transaction(
								txn, subspace, &id,
							)
							.await
						}))
						.await?;
					let mut relations = Vec::with_capacity(results.len());
					for result in results {
						let relation = match result {
							ControlFlow::Break(relation) => relation,
							ControlFlow::Continue(error) => {
								return Ok(ControlFlow::Continue(error));
							},
						};
						relations.push(relation);
					}

					relations
				};

				let mut next = Vec::new();
				for (groups, organizations) in relations {
					for group in groups {
						let id = tg::Id::from(group.clone());
						if visited.insert(id.clone()) {
							subjects.push(tg::authorization::Subject::Group(group));
							next.push(id);
						}
					}
					for organization in organizations {
						let id = tg::Id::from(organization.clone());
						if visited.insert(id.clone()) {
							subjects.push(tg::authorization::Subject::Organization(organization));
							next.push(id);
						}
					}
				}
				frontier = next;
			}
		}

		Ok(ControlFlow::Break(subjects))
	}
}
