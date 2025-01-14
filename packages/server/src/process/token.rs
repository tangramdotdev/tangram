use crate::Server;
use tangram_client as tg;

impl Server {
    pub(crate) async fn try_create_process_token(&self, process: &tg::process::Id) -> tg::Result<Option<String>> {
        todo!()
    }
}