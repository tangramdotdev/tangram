create table if not exists objects (
	bytes blob,
	cache_pointer blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists memberships (
	id blob,
	kind tinyint,
	namespace text,
	stored_at bigint,
	primary key (id, kind, namespace)
);
