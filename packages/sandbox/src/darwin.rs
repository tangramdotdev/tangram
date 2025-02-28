use crate::{Child, ExitStatus};
impl Child {
    pub(crate) async fn wait_darwin(&mut self) -> std::io::Result<ExitStatus> {
        todo!()
    }
}