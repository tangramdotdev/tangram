create table if not exists objects (
	id blob primary key,
	bytes blob,
	cache_reference blob,
	touched_at bigint
);

create table if not exists logs (
	id blob,
	index bigint,
	bytes blob,
	stream bigint,
	combined_position bigint,
	stderr_position bigint,
	stdout_position bigint,
	timestamp bigint,
	primary key (id, index)
);

