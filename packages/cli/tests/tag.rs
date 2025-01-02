use indoc::indoc;
use insta::{assert_json_snapshot, assert_snapshot};
use std::{collections::BTreeMap, future::Future, path::Path};
use tangram_cli::{
	assert_output_success,
	test::{test, Server},
};
use tangram_client as tg;
use tangram_temp::{self as temp, Temp};

const TG: &str = env!("CARGO_BIN_EXE_tangram");

#[tokio::test]
async fn list_no_results() -> tg::Result<()> {
	todo!()
}

#[tokio::test]
async fn get_no_results() -> tg::Result<()> {
	todo!()
}

#[tokio::test]
async fn single() -> tg::Result<()> {
	todo!()
}

#[tokio::test]
async fn multiple() -> tg::Result<()> {
	todo!()
}

#[tokio::test]
async fn remote_put() -> tg::Result<()> {
	todo!()
}
