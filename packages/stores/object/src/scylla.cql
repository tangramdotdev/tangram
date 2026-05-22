create table if not exists objects (
	bytes blob,
	cache_pointer blob,
	id blob primary key,
	stored_at bigint
);
