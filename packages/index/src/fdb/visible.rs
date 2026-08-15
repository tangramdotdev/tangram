use {
	crate::fdb::Index,
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::{
		collections::{HashSet, VecDeque},
		ops::ControlFlow,
	},
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
		txn: &fdb::Transaction,
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
		let mut output = Vec::with_capacity(ids.len());
		for id in ids {
			let mut visible = false;
			for subject in &subjects {
				if crate::fdb::propagate!(
					Self::try_get_visibility_with_transaction(txn, subspace, id, subject).await
				) {
					visible = true;
					break;
				}
			}
			output.push(visible);
		}
		Ok(ControlFlow::Break(output))
	}

	pub(crate) async fn requester_subjects_with_transaction(
		txn: &fdb::Transaction,
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
			let mut queue = VecDeque::from([id.clone()]);
			let mut visited = HashSet::from([id]);
			while let Some(id) = queue.pop_front() {
				let groups = crate::fdb::propagate!(
					Self::get_member_groups_with_transaction(txn, subspace, &id).await
				);
				for group in groups {
					let id = tg::Id::from(group.clone());
					if visited.insert(id.clone()) {
						subjects.push(tg::authorization::Subject::Group(group));
						queue.push_back(id);
					}
				}
				let organizations = crate::fdb::propagate!(
					Self::get_member_organizations_with_transaction(txn, subspace, &id).await
				);
				for organization in organizations {
					let id = tg::Id::from(organization.clone());
					if visited.insert(id.clone()) {
						subjects.push(tg::authorization::Subject::Organization(organization));
						queue.push_back(id);
					}
				}
			}
		}
		Ok(ControlFlow::Break(subjects))
	}
}
