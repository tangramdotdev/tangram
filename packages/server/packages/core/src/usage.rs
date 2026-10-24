use {
	crate::{Session, database::Transaction},
	futures::future::BoxFuture,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

/// The usage component.
pub trait Component: crate::component::Lifecycle {
	fn get_usage_regions<'a>(
		&'a self,
		session: &'a Session,
		account: &'a tg::usage::Account,
		period: tg::usage::Period,
		tokens: &'a tg::authorization::Tokens,
	) -> BoxFuture<'a, tg::Result<tg::usage::Output>>;

	fn get_usage_local<'a>(
		&'a self,
		session: &'a Session,
		account: &'a tg::usage::Account,
		period: tg::usage::Period,
	) -> BoxFuture<'a, tg::Result<tg::usage::Output>>;

	fn usage_account_for_specifier_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a Transaction<'_>,
		specifier: &'a tg::Specifier,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::usage::Account>, crate::database::Error>>>;

	fn usage_account_with_transaction<'a>(
		&'a self,
		session: &'a Session,
		transaction: &'a Transaction<'_>,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<ControlFlow<Option<tg::usage::Account>, crate::database::Error>>>;

	fn usage_account<'a>(
		&'a self,
		session: &'a Session,
		principal: &'a tg::Principal,
	) -> BoxFuture<'a, tg::Result<Option<tg::usage::Account>>>;
}
