create table if not exists objects (
	id blob primary key,
	bytes blob,
	cache_reference blob,
	touched_at bigint
);
