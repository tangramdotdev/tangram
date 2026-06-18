pub use self::store::Store;

use {
	crate::Session,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

pub mod batch;
pub mod get;
pub mod metadata;
pub mod put;
pub mod store;
pub mod touch;

impl Session {
	pub(crate) async fn object_children_are_subtree_authorized(
		&self,
		children: &BTreeSet<tg::object::Id>,
		advisory_children: &[tg::MaybeWithToken<tg::object::Id>],
		batch_subtrees: &BTreeSet<tg::object::Id>,
		batch_objects: &BTreeSet<tg::object::Id>,
	) -> tg::Result<bool> {
		let advisory_children = Self::usable_advisory_children(children, advisory_children);
		for child in children {
			if batch_objects.contains(child) {
				if !batch_subtrees.contains(child) {
					return Ok(false);
				}
				continue;
			}
			let Some(advisory_children) = &advisory_children else {
				return Ok(false);
			};
			let child = advisory_children.get(child).unwrap().clone();
			if !self.object_token_grants_subtree(child) {
				return Ok(false);
			}
		}
		Ok(true)
	}

	pub(crate) fn object_output(
		&self,
		id: tg::object::Id,
		permission: tg::grant::permission::object::Permission,
		expires_at: i64,
	) -> tg::Result<tg::MaybeWithToken<tg::object::Id>> {
		let token = self.create_token(
			tg::grant::Resource::Id(id.clone().into()),
			vec![tg::grant::Permission::Object(permission)],
			expires_at,
		)?;
		let output = if let Some(token) = token {
			tg::Either::Right(tg::WithToken { id, token })
		} else {
			tg::Either::Left(id)
		};
		Ok(output)
	}

	fn usable_advisory_children(
		children: &BTreeSet<tg::object::Id>,
		advisory_children: &[tg::MaybeWithToken<tg::object::Id>],
	) -> Option<BTreeMap<tg::object::Id, tg::MaybeWithToken<tg::object::Id>>> {
		if children.len() != advisory_children.len() {
			return None;
		}
		let mut map = BTreeMap::new();
		for child in advisory_children {
			let id = match child {
				tg::Either::Left(id) => id.clone(),
				tg::Either::Right(child) => child.id.clone(),
			};
			if map.insert(id, child.clone()).is_some() {
				return None;
			}
		}
		(map.keys().eq(children.iter())).then_some(map)
	}

	fn object_token_grants_subtree(&self, object: tg::MaybeWithToken<tg::object::Id>) -> bool {
		let tg::Either::Right(object) = object else {
			return false;
		};
		let resource = tg::grant::Resource::Id(object.id.into());
		let permission =
			tg::grant::Permission::Object(tg::grant::permission::object::Permission::Subtree);
		self.authorize_token(&resource, permission.into(), &object.token)
	}
}
