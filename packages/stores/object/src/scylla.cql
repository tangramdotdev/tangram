create table if not exists objects (
	bytes blob,
	cache_pointer blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists object_grants (
	object blob,
	principal text,
	subtree boolean,
	created_at bigint,
	primary key (object, principal, subtree)
);
