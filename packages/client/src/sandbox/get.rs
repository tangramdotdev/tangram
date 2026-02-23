use { crate::prelude::*, std::path::PathBuf };

pub struct Arg {
    pub id: tg::sandbox::Id,
}

pub struct Output {
    pub path: PathBuf,
}
