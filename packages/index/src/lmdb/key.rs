use {
	foundationdb_tuple as fdbt,
	num_traits::{FromPrimitive as _, ToPrimitive as _},
	tangram_client::prelude::*,
};

#[derive(Debug)]
pub enum Key {
	Cache(crate::lmdb::cache::Key),
	Clean(crate::lmdb::clean::Key),
	Object(crate::lmdb::object::Key),
	Process(crate::lmdb::process::Key),
	Tag(crate::lmdb::tag::Key),
	Update(crate::lmdb::update::Key),
}

#[derive(Clone, Copy, Debug, PartialEq, num_derive::FromPrimitive, num_derive::ToPrimitive)]
#[repr(u8)]
pub enum Kind {
	CacheEntry = 0,
	Object = 1,
	Process = 2,
	Tag = 3,
	CacheEntryDependency = 4,
	DependencyCacheEntry = 5,
	ObjectChild = 6,
	ChildObject = 7,
	ObjectCacheEntry = 8,
	CacheEntryObject = 9,
	ProcessChild = 10,
	ChildProcess = 11,
	ProcessObject = 12,
	ObjectProcess = 13,
	ItemTag = 14,
	Clean = 15,
	Update = 16,
	UpdateVersion = 17,
	ParentTag = 18,
	TagParent = 19,
}

impl fdbt::TuplePack for Key {
	fn pack<W: std::io::Write>(
		&self,
		w: &mut W,
		tuple_depth: fdbt::TupleDepth,
	) -> std::io::Result<fdbt::VersionstampOffset> {
		match self {
			Key::Cache(crate::lmdb::cache::Key::CacheEntry(id)) => {
				(Kind::CacheEntry.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Object(crate::lmdb::object::Key::Object(id)) => {
				(Kind::Object.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Process(crate::lmdb::process::Key::Process(id)) => {
				(Kind::Process.to_i32().unwrap(), id.to_bytes().as_ref()).pack(w, tuple_depth)
			},

			Key::Tag(crate::lmdb::tag::Key::Tag(id)) => {
				(Kind::Tag.to_i32().unwrap(), id.to_string()).pack(w, tuple_depth)
			},

			Key::Cache(crate::lmdb::cache::Key::CacheEntryDependency {
				cache_entry,
				dependency,
			}) => (
				Kind::CacheEntryDependency.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				dependency.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Cache(crate::lmdb::cache::Key::DependencyCacheEntry {
				dependency,
				cache_entry,
			}) => (
				Kind::DependencyCacheEntry.to_i32().unwrap(),
				dependency.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ObjectChild { object, child }) => (
				Kind::ObjectChild.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ChildObject { child, object }) => (
				Kind::ChildObject.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ObjectCacheEntry {
				object,
				cache_entry,
			}) => (
				Kind::ObjectCacheEntry.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				cache_entry.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::CacheEntryObject {
				cache_entry,
				object,
			}) => (
				Kind::CacheEntryObject.to_i32().unwrap(),
				cache_entry.to_bytes().as_ref(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ProcessChild { process, child }) => (
				Kind::ProcessChild.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				child.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ChildProcess { child, parent }) => (
				Kind::ChildProcess.to_i32().unwrap(),
				child.to_bytes().as_ref(),
				parent.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Process(crate::lmdb::process::Key::ProcessObject {
				process,
				kind,
				object,
			}) => (
				Kind::ProcessObject.to_i32().unwrap(),
				process.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				object.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Object(crate::lmdb::object::Key::ObjectProcess {
				object,
				kind,
				process,
			}) => (
				Kind::ObjectProcess.to_i32().unwrap(),
				object.to_bytes().as_ref(),
				kind.to_i32().unwrap(),
				process.to_bytes().as_ref(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::ItemTag { item, tag }) => (
				Kind::ItemTag.to_i32().unwrap(),
				item.as_slice(),
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::ParentTag { parent, name, tag }) => (
				Kind::ParentTag.to_i32().unwrap(),
				parent.as_ref().map(ToString::to_string),
				name,
				tag.to_string(),
			)
				.pack(w, tuple_depth),

			Key::Tag(crate::lmdb::tag::Key::TagParent { tag, parent, name }) => (
				Kind::TagParent.to_i32().unwrap(),
				tag.to_string(),
				parent.as_ref().map(ToString::to_string),
				name,
			)
				.pack(w, tuple_depth),

			Key::Clean(crate::lmdb::clean::Key::Clean {
				touched_at,
				kind,
				id,
			}) => {
				Kind::Clean.to_i32().unwrap().pack(w, tuple_depth)?;
				touched_at.pack(w, tuple_depth)?;
				kind.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::Update(crate::lmdb::update::Key::Update { id }) => {
				Kind::Update.to_i32().unwrap().pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},

			Key::Update(crate::lmdb::update::Key::UpdateVersion { version, id }) => {
				Kind::UpdateVersion.to_i32().unwrap().pack(w, tuple_depth)?;
				version.pack(w, tuple_depth)?;
				let id = match &id {
					tg::Either::Left(id) => id.to_bytes(),
					tg::Either::Right(id) => id.to_bytes(),
				};
				id.as_ref().pack(w, tuple_depth)
			},
		}
	}
}

impl fdbt::TupleUnpack<'_> for Key {
	fn unpack(input: &[u8], tuple_depth: fdbt::TupleDepth) -> fdbt::PackResult<(&[u8], Self)> {
		let (input, kind_value) = i32::unpack(input, tuple_depth)?;
		let kind =
			Kind::from_i32(kind_value).ok_or(fdbt::PackError::Message("invalid kind".into()))?;

		match kind {
			Kind::CacheEntry => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::artifact::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				Ok((input, Key::Cache(crate::lmdb::cache::Key::CacheEntry(id))))
			},

			Kind::Object => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::object::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((input, Key::Object(crate::lmdb::object::Key::Object(id))))
			},

			Kind::Process => {
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::process::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((input, Key::Process(crate::lmdb::process::Key::Process(id))))
			},

			Kind::Tag => {
				let (input, id): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = id
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((input, Key::Tag(crate::lmdb::tag::Key::Tag(id))))
			},

			Kind::CacheEntryDependency => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Cache(crate::lmdb::cache::Key::CacheEntryDependency {
					cache_entry,
					dependency,
				});
				Ok((input, key))
			},

			Kind::DependencyCacheEntry => {
				let (input, dependency_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let dependency = tg::artifact::Id::from_slice(&dependency_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Cache(crate::lmdb::cache::Key::DependencyCacheEntry {
					dependency,
					cache_entry,
				});
				Ok((input, key))
			},

			Kind::ObjectChild => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((
					input,
					Key::Object(crate::lmdb::object::Key::ObjectChild { object, child }),
				))
			},

			Kind::ChildObject => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::object::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				Ok((
					input,
					Key::Object(crate::lmdb::object::Key::ChildObject { child, object }),
				))
			},

			Kind::ObjectCacheEntry => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let key = Key::Object(crate::lmdb::object::Key::ObjectCacheEntry {
					object,
					cache_entry,
				});
				Ok((input, key))
			},

			Kind::CacheEntryObject => {
				let (input, cache_entry_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let cache_entry = tg::artifact::Id::from_slice(&cache_entry_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid artifact id".into()))?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Object(crate::lmdb::object::Key::CacheEntryObject {
					cache_entry,
					object,
				});
				Ok((input, key))
			},

			Kind::ProcessChild => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Process(crate::lmdb::process::Key::ProcessChild { process, child }),
				))
			},

			Kind::ChildProcess => {
				let (input, child_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, parent_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let child = tg::process::Id::from_slice(&child_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let parent = tg::process::Id::from_slice(&parent_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				Ok((
					input,
					Key::Process(crate::lmdb::process::Key::ChildProcess { child, parent }),
				))
			},

			Kind::ProcessObject => {
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let kind = crate::process::object::Kind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let key = Key::Process(crate::lmdb::process::Key::ProcessObject {
					process,
					kind,
					object,
				});
				Ok((input, key))
			},

			Kind::ObjectProcess => {
				let (input, object_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value) = i32::unpack(input, tuple_depth)?;
				let (input, process_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let object = tg::object::Id::from_slice(&object_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
				let kind = crate::process::object::Kind::from_i32(kind_value).ok_or(
					fdbt::PackError::Message("invalid process object kind".into()),
				)?;
				let process = tg::process::Id::from_slice(&process_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
				let key = Key::Object(crate::lmdb::object::Key::ObjectProcess {
					object,
					kind,
					process,
				});
				Ok((input, key))
			},

			Kind::ItemTag => {
				let (input, item): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((
					input,
					Key::Tag(crate::lmdb::tag::Key::ItemTag { item, tag }),
				))
			},

			Kind::ParentTag => {
				let (input, parent): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, name): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let parent = parent
					.map(|parent| {
						parent
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid parent id".into()))
					})
					.transpose()?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				Ok((
					input,
					Key::Tag(crate::lmdb::tag::Key::ParentTag { parent, name, tag }),
				))
			},

			Kind::TagParent => {
				let (input, tag): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, parent): (_, Option<String>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, name): (_, String) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let tag = tag
					.parse()
					.map_err(|_| fdbt::PackError::Message("invalid tag id".into()))?;
				let parent = parent
					.map(|parent| {
						parent
							.parse()
							.map_err(|_| fdbt::PackError::Message("invalid parent id".into()))
					})
					.transpose()?;
				Ok((
					input,
					Key::Tag(crate::lmdb::tag::Key::TagParent { tag, parent, name }),
				))
			},

			Kind::Clean => {
				let (input, touched_at): (_, i64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, kind_value): (_, i32) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let kind = crate::lmdb::clean::ItemKind::from_i32(kind_value)
					.ok_or(fdbt::PackError::Message("invalid item kind".into()))?;
				let (input, id_bytes): (_, Vec<u8>) =
					fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id_bytes)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = match kind {
					crate::lmdb::clean::ItemKind::CacheEntry
					| crate::lmdb::clean::ItemKind::Object => {
						let id = tg::object::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid object id".into()))?;
						tg::Either::Left(id)
					},
					crate::lmdb::clean::ItemKind::Process => {
						let id = tg::process::Id::try_from(id)
							.map_err(|_| fdbt::PackError::Message("invalid process id".into()))?;
						tg::Either::Right(id)
					},
				};
				let key = Key::Clean(crate::lmdb::clean::Key::Clean {
					touched_at,
					kind,
					id,
				});
				Ok((input, key))
			},

			Kind::Update => {
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				Ok((input, Key::Update(crate::lmdb::update::Key::Update { id })))
			},

			Kind::UpdateVersion => {
				let (input, version): (_, u64) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let (input, id): (_, Vec<u8>) = fdbt::TupleUnpack::unpack(input, tuple_depth)?;
				let id = tg::Id::from_slice(&id)
					.map_err(|_| fdbt::PackError::Message("invalid id".into()))?;
				let id = if let Ok(id) = tg::process::Id::try_from(id.clone()) {
					tg::Either::Right(id)
				} else if let Ok(id) = tg::object::Id::try_from(id) {
					tg::Either::Left(id)
				} else {
					return Err(fdbt::PackError::Message("invalid id".into()));
				};
				Ok((
					input,
					Key::Update(crate::lmdb::update::Key::UpdateVersion { version, id }),
				))
			},
		}
	}
}
