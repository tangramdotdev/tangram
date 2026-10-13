use {crate::run::linux::Arg, std::path::Path, tangram_client::prelude::*};

pub(crate) fn exec(_arg: &Arg, _root: &Path) -> tg::Result<()> {
    Err(tg::error!("the gvisor launcher is unavailable"))
}
