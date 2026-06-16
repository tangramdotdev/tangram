mod delete;
mod get;
mod key;
mod put;

pub(super) use key::Key;

#[derive(Clone, Copy)]
pub(crate) enum GrantSource {
	Explicit,
	Materialized,
	All,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct GrantEntry {
	pub expires_at: Option<i64>,
	pub permission: tangram_client::grant::Permission,
	pub principal: tangram_client::grant::Principal,
	pub sources: u8,
}

#[derive(Clone)]
pub(crate) struct GrantIndexEntry<'a> {
	pub expires_at: Option<i64>,
	pub permission: tangram_client::grant::Permission,
	pub principal: &'a tangram_client::grant::Principal,
	pub resource: &'a tangram_client::Id,
}

pub(crate) const EXPLICIT_GRANT_SOURCE: u8 = 1 << 0;
pub(crate) const MATERIALIZED_GRANT_SOURCE: u8 = 1 << 1;

pub(crate) fn grant_sources(bytes: &[u8]) -> u8 {
	bytes.first().copied().unwrap_or(EXPLICIT_GRANT_SOURCE)
}

pub(crate) fn grant_source_mask(source: GrantSource) -> u8 {
	match source {
		GrantSource::Explicit => EXPLICIT_GRANT_SOURCE,
		GrantSource::Materialized => MATERIALIZED_GRANT_SOURCE,
		GrantSource::All => EXPLICIT_GRANT_SOURCE | MATERIALIZED_GRANT_SOURCE,
	}
}
