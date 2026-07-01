create table if not exists objects (
	bytes blob,
	cache_pointer blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists object_outbox (
	partition bigint,
	token timeuuid,
	id blob,
	grant_payload blob,
	metadata_payload blob,
	primary key ((partition), token)
);
