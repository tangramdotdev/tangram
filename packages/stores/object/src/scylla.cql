create table if not exists objects (
	id blob primary key,
	bytes blob,
	cache_pointer blob,
	touched_at bigint
);
