use {
	std::{collections::BTreeMap, sync::Arc},
	tangram_client::prelude::*,
};

#[derive(Clone, Default)]
pub struct Solutions {
	map: im::HashMap<tg::specifier::Pattern, Solution, fnv::FnvBuildHasher>,
	referents: im::HashMap<
		usize,
		im::HashSet<tg::specifier::Pattern, fnv::FnvBuildHasher>,
		fnv::FnvBuildHasher,
	>,
	referrers: im::HashMap<
		usize,
		im::HashSet<tg::specifier::Pattern, fnv::FnvBuildHasher>,
		fnv::FnvBuildHasher,
	>,
}

#[derive(Clone)]
pub struct Solution {
	pub candidate_options: ObjectOptions,
	pub referent: Option<tg::Referent<tg::graph::data::Edge<tg::object::Id>>>,
	pub referrers: Vec<Referrer>,
}

#[derive(Clone)]
pub struct Referrer {
	pub index: usize,
	pub pattern: Option<tg::specifier::Pattern>,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectOptions {
	pub children: Option<Arc<BTreeMap<tg::object::Id, tg::object::get::Child>>>,
	pub location: Option<tg::location::Arg>,
	pub tokens: tg::authorization::Tokens,
}

impl Solutions {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.map.is_empty()
	}

	#[must_use]
	pub fn get(&self, key: &tg::specifier::Pattern) -> Option<&Solution> {
		self.map.get(key)
	}

	pub fn insert(&mut self, key: tg::specifier::Pattern, solution: Solution) {
		if let Some(existing) = self.map.get(&key)
			&& let Some(referent) = &existing.referent
			&& let Some(target_index) = referent.node().try_unwrap_index_ref().ok()
			&& let Some(patterns) = self.referents.get_mut(target_index)
		{
			patterns.remove(&key);
			if patterns.is_empty() {
				self.referents.remove(target_index);
			}
		}
		if let Some(referent) = &solution.referent
			&& let Some(target_index) = referent.node().try_unwrap_index_ref().ok()
		{
			self.referents
				.entry(*target_index)
				.or_default()
				.insert(key.clone());
		}
		self.map.insert(key, solution);
	}

	pub fn remove(&mut self, key: &tg::specifier::Pattern) -> Option<Solution> {
		let solution = self.map.remove(key)?;
		let Some(referent) = &solution.referent else {
			return Some(solution);
		};
		let Some(target_index) = referent.node().try_unwrap_index_ref().ok() else {
			return Some(solution);
		};
		if let Some(patterns) = self.referents.get_mut(target_index) {
			patterns.remove(key);
			if patterns.is_empty() {
				self.referents.remove(target_index);
			}
		}
		for referrer in &solution.referrers {
			if let Some(patterns) = self.referrers.get_mut(&referrer.index) {
				patterns.remove(key);
				if patterns.is_empty() {
					self.referrers.remove(&referrer.index);
				}
			}
		}
		Some(solution)
	}

	pub fn clear(&mut self) {
		self.map.clear();
		self.referents.clear();
		self.referrers.clear();
	}

	pub fn remove_by_node(&mut self, node: usize) {
		if let Some(patterns) = self.referents.remove(&node) {
			for pattern in &patterns {
				if let Some(solution) = self.map.remove(pattern) {
					for referrer in solution.referrers {
						if let Some(referrer_patterns) = self.referrers.get_mut(&referrer.index) {
							referrer_patterns.remove(pattern);
							if referrer_patterns.is_empty() {
								self.referrers.remove(&referrer.index);
							}
						}
					}
				}
			}
		}

		if let Some(patterns) = self.referrers.remove(&node) {
			let mut to_remove = Vec::new();
			for pattern in patterns {
				if let Some(solution) = self.map.get_mut(&pattern) {
					solution.referrers.retain(|r| r.index != node);
					if solution.referrers.is_empty() {
						to_remove.push(pattern);
					}
				}
			}
			for pattern in to_remove {
				if let Some(solution) = self.map.remove(&pattern)
					&& let Some(referent) = &solution.referent
					&& let Some(target_index) = referent.node().try_unwrap_index_ref().ok()
					&& let Some(referent_patterns) = self.referents.get_mut(target_index)
				{
					referent_patterns.remove(&pattern);
					if referent_patterns.is_empty() {
						self.referents.remove(target_index);
					}
				}
			}
		}
	}

	pub fn clear_referent(&mut self, key: &tg::specifier::Pattern) {
		if let Some(solution) = self.map.get_mut(key)
			&& let Some(referent) = solution.referent.take()
			&& let Some(target_index) = referent.node().try_unwrap_index_ref().ok()
			&& let Some(patterns) = self.referents.get_mut(target_index)
		{
			patterns.remove(key);
			if patterns.is_empty() {
				self.referents.remove(target_index);
			}
		}
	}

	pub fn add_referrer(&mut self, key: &tg::specifier::Pattern, referrer: Referrer) {
		self.referrers
			.entry(referrer.index)
			.or_default()
			.insert(key.clone());
		if let Some(solution) = self.map.get_mut(key) {
			solution.referrers.push(referrer);
		}
	}
}

impl ObjectOptions {
	#[must_use]
	pub fn from_dependency(
		reference: &tg::Reference,
		dependency: &tg::graph::data::Dependency,
	) -> Self {
		let location = dependency
			.options
			.location
			.clone()
			.map(Into::into)
			.or_else(|| reference.options().location.clone());
		let mut tokens = dependency.options.tokens.clone();
		tokens.inherit(&reference.options().tokens);
		Self {
			children: None,
			location,
			tokens,
		}
	}

	#[must_use]
	pub fn from_reference(reference: &tg::Reference) -> Self {
		Self {
			children: None,
			location: reference.options().location.clone(),
			tokens: reference.options().tokens.clone(),
		}
	}

	#[must_use]
	pub fn with_location_and_tokens(
		location: Option<tg::Location>,
		tokens: tg::authorization::Tokens,
	) -> Self {
		let location = location.map(Into::into);
		Self {
			children: None,
			location,
			tokens,
		}
	}

	#[must_use]
	pub fn from_referent_options(options: &tg::referent::Options) -> Self {
		Self {
			children: None,
			location: options.location.clone().map(Into::into),
			tokens: options.tokens.clone(),
		}
	}

	pub fn inherit(&mut self, parent: &Self) {
		if self.children.is_none() {
			self.children.clone_from(&parent.children);
		}
		self.tokens.inherit(&parent.tokens);
		if self.location.is_none() {
			self.location.clone_from(&parent.location);
		}
	}

	pub fn update_from_output(&mut self, output: &tg::object::get::Output) {
		self.children = Some(Arc::new(output.children.clone()));
		if !output.tokens.is_empty() {
			self.tokens.clone_from(&output.tokens);
		}
	}

	#[must_use]
	pub fn tokens_for_object(&self, id: &tg::object::Id) -> tg::authorization::Tokens {
		let Some(child) = self.children.as_ref().and_then(|children| children.get(id)) else {
			return self.tokens.clone();
		};
		let mut tokens = child.tokens.clone();
		tokens.inherit(&self.tokens);
		tokens
	}
}
