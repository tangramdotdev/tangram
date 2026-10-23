use {
	crate::Component,
	bytes::Bytes,
	futures::FutureExt as _,
	num::ToPrimitive as _,
	std::{io::Cursor, sync::Arc},
	tangram_client::prelude::*,
	tangram_server_core::{
		Session,
		object::CheckoutFile,
		read::{File, Object, ReadFuture, Reader},
	},
};

struct Loader {
	checkout_file: Option<CheckoutFile>,
	session: Session,
}

impl Component {
	pub(crate) async fn reader(&self, session: &Session, blob: tg::Blob) -> tg::Result<Reader> {
		let id = blob.id();
		let checkout_pointer = if session.server.checkouts_enabled() {
			let arg = tangram_server_core::cache::object::get::Arg {
				bytes: true,
				id: id.clone().into(),
				put: None,
			};
			session
				.server
				.cache
				.try_get_object(arg)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
				.object
				.and_then(|object| object.checkout_pointer)
		} else {
			None
		};
		let checkout_pointer = if let Some(checkout_pointer) = checkout_pointer {
			let permission = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			);
			let resource = tg::Referent::with_node_and_tokens(
				tg::object::Id::from(id.clone()),
				blob.state().tokens(),
			);
			let authorized = session
				.authorize(resource, permission)
				.await?
				.permissions
				.contains(permission);
			authorized.then_some(checkout_pointer)
		} else {
			None
		};
		let reader = if let Some(checkout_pointer) = checkout_pointer {
			let mut path = session
				.server
				.checkout_path()
				.join(checkout_pointer.artifact.to_string());
			if let Some(path_) = &checkout_pointer.path {
				path.push(path_);
			}
			let file = tokio::fs::File::open(&path).await.map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to open the file"),
			)?;
			let reader = File::new(file, checkout_pointer.position, checkout_pointer.length)
				.await
				.map_err(|error| tg::error!(!error, %id, "failed to create the file reader"))?;
			Reader::File(reader)
		} else {
			let length = blob
				.length_with_instance(session)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the blob length"))
				.map_err(|error| tg::error!(!error, %id, "failed to create the object reader"))?;
			let loader = Loader::new(session);
			let reader = Object::new(blob, length, Box::new(loader));
			Reader::Object(reader)
		};
		Ok(reader)
	}

	pub(crate) fn reader_sync(session: &Session, blob: tg::Blob) -> tg::Result<Reader> {
		let id = blob.id();
		let arg = tangram_server_core::cache::object::get::Arg {
			bytes: true,
			id: id.clone().into(),
			put: None,
		};
		let object = session
			.server
			.cache
			.try_get_object_sync(&arg)
			.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?;
		let checkout_pointer = session
			.server
			.checkouts_enabled()
			.then(|| object.object.and_then(|object| object.checkout_pointer))
			.flatten();
		let reader = if let Some(checkout_pointer) = checkout_pointer {
			let mut path = session
				.server
				.checkout_path()
				.join(checkout_pointer.artifact.to_string());
			if let Some(path_) = &checkout_pointer.path {
				path.push(path_);
			}
			let file = std::fs::File::open(&path).map_err(
				|error| tg::error!(!error, path = %path.display(), "failed to open the file"),
			)?;
			let reader =
				File::new_sync(file, checkout_pointer.position, checkout_pointer.length)
					.map_err(|error| tg::error!(!error, %id, "failed to create the file reader"))?;
			Reader::File(reader)
		} else {
			let mut file = None;
			let Some(output) = session
				.server
				.components
				.object
				.try_get_object_sync(&session.server, &id.clone().into(), &mut file)
				.map_err(|error| tg::error!(!error, %id, "failed to get the object"))?
			else {
				return Err(tg::error!(%id, "failed to get the blob object"));
			};
			let data = tg::blob::Data::deserialize(output.bytes)
				.map_err(|error| tg::error!(!error, %id, "failed to deserialize the blob data"))?;
			let object = tg::blob::Object::try_from_data(data)
				.map_err(|error| tg::error!(!error, %id, "failed to create the blob object"))?;
			let length = match &object {
				tg::blob::Object::Leaf(leaf) => leaf.bytes.len().to_u64().unwrap(),
				tg::blob::Object::Branch(branch) => {
					branch.children.iter().map(|child| child.length).sum()
				},
			};
			let loader = Loader::new(session);
			let reader = Object::new(blob, length, Box::new(loader));
			Reader::Object(reader)
		};
		Ok(reader)
	}
}

impl Loader {
	fn new(session: &Session) -> Self {
		let session = session.clone();
		Self {
			checkout_file: None,
			session,
		}
	}
}

impl tangram_server_core::read::Loader for Loader {
	fn read(&self, blob: tg::Blob, position: u64) -> ReadFuture {
		let session = self.session.clone();
		async move { read(&session, blob, position).await }.boxed()
	}

	fn read_sync(&mut self, blob: &tg::Blob, position: u64) -> tg::Result<Option<Cursor<Bytes>>> {
		read_sync(&self.session, blob, position, &mut self.checkout_file)
	}
}

fn read_sync(
	session: &Session,
	blob: &tg::Blob,
	position: u64,
	checkout_file: &mut Option<CheckoutFile>,
) -> tg::Result<Option<Cursor<Bytes>>> {
	let mut current_blob = blob.clone();
	let mut current_blob_position = 0;
	'a: loop {
		let state = current_blob.state();
		let id = state.try_get_id();
		let object = state
			.object()
			.map(|object| object.unwrap_blob_ref().clone());
		let object = if let Some(object) = object {
			object
		} else {
			let Some(output) = session
				.server
				.components
				.object
				.try_get_object_sync(&session.server, &id.unwrap(), checkout_file)
				.map_err(|error| tg::error!(!error, "failed to get the object"))?
			else {
				return Err(tg::error!("failed to get the blob object"));
			};
			let data = tg::blob::Data::deserialize(output.bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the blob"))?;
			let object = tg::blob::Object::try_from_data(data)
				.map_err(|error| tg::error!(!error, "failed to create the blob object"))?;
			let object = Arc::new(object);
			if object.is_branch() {
				state.set_object(object.clone());
			}
			object
		};
		match object.as_ref() {
			tg::blob::Object::Leaf(leaf) => {
				if position < current_blob_position + leaf.bytes.len().to_u64().unwrap() {
					let mut cursor = Cursor::new(leaf.bytes.clone());
					cursor.set_position(position - current_blob_position);
					break Ok(Some(cursor));
				}
				return Ok(None);
			},
			tg::blob::Object::Branch(branch) => {
				for child in &branch.children {
					if position < current_blob_position + child.length {
						current_blob = child.blob.clone();
						continue 'a;
					}
					current_blob_position += child.length;
				}
				return Ok(None);
			},
		}
	}
}

async fn read(
	session: &Session,
	blob: tg::Blob,
	position: u64,
) -> tg::Result<Option<Cursor<Bytes>>> {
	let mut current_blob = blob.clone();
	let mut current_blob_position = 0;
	'a: loop {
		let state = current_blob.state();
		let id = state.try_get_id();
		let object = state
			.object()
			.map(|object| object.unwrap_blob_ref().clone());
		let object = if let Some(object) = object {
			object
		} else {
			let arg = tg::object::get::Arg {
				tokens: state.tokens(),
				..Default::default()
			};
			let output = session
				.get_object(&id.unwrap(), arg)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the object"))?;
			state.inherit_tokens(&output.tokens);
			let data = tg::blob::Data::deserialize(output.bytes)
				.map_err(|error| tg::error!(!error, "failed to deserialize the blob"))?;
			let object = tg::blob::Object::try_from_data(data)
				.map_err(|error| tg::error!(!error, "failed to create the blob object"))?;
			for child in object.children() {
				if let Some(output) = output.children.get(&child.id()) {
					child.state().inherit_tokens(&output.tokens);
				}
			}
			let object = Arc::new(object);
			if object.is_branch() {
				state.set_object(object.clone());
			}
			object
		};
		match object.as_ref() {
			tg::blob::Object::Leaf(leaf) => {
				if position < current_blob_position + leaf.bytes.len().to_u64().unwrap() {
					let mut cursor = Cursor::new(leaf.bytes.clone());
					cursor.set_position(position - current_blob_position);
					break Ok(Some(cursor));
				}
				return Ok(None);
			},
			tg::blob::Object::Branch(branch) => {
				for child in &branch.children {
					if position < current_blob_position + child.length {
						current_blob = child.blob.clone();
						continue 'a;
					}
					current_blob_position += child.length;
				}
				return Ok(None);
			},
		}
	}
}
