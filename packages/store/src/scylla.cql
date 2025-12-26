create table if not exists objects (
	id blob primary key,
	bytes blob,
	cache_reference blob,
	touched_at bigint
);

create table if not exists logs (
	id blob,
	log_index bigint,
	bytes blob,
	stream bigint,
	combined_start bigint,
	combined_end bigint,
	stdout_start bigint,
	stdout_end bigint,
	stderr_start bigint,
	stderr_end bigint,
	timestamp bigint,
	primary key (id, log_index)
);
