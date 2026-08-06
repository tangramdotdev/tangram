create table if not exists objects (
	bytes blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists outbox (
	partition bigint,
	id timeuuid,
	payload blob,
	primary key ((partition), id)
) with clustering order by (id asc);
