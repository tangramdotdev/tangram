create table if not exists objects (
	bytes blob,
	cache_pointer blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists object_outbox (
	partition bigint,
	id blob,
	enqueued_at bigint,
	payload blob,
	primary key (partition, id)
);
