use crate::{document, Module};
use tangram_error::Result;

impl Module {
	pub async fn version(&self, document_store: Option<&document::Store>) -> Result<i32> {
		match self {
			Module::Library(_) | Module::Normal { .. } => Ok(0),
			Module::Document(document) => document.version(document_store.unwrap()).await,
		}
	}
}
