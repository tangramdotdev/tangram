create table if not exists objects (
	id blob primary key,
	bytes blob,
	cache_pointer blob,
	touched_at bigint
);

create table if not exists process_log_entries (
	process blob,
	position bigint,
	bytes blob,
	stream int,
	stream_position bigint,
	timestamp bigint,
	primary key (process, position)
);

create table if not exists process_log_stream_positions (
	process blob,
	stream int,
	stream_position bigint,
	position bigint,
	primary key (process, stream, stream_position)
);
