use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use std::sync::Arc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub(crate) async fn try_put_process_pty(
		&self,
		id: &tg::process::Id,
		arg: tg::process::pty::put::Arg,
	) -> tg::Result<()> {
		todo!()
	}
}

impl Server {
	pub(crate) async fn handle_try_put_process_pty_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		let output = handle.try_put_process_pty(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
