create table if not exists objects (
	bytes blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists outbox (
	batch blob,
	fragment bigint,
	partition bigint,
	payload blob,
	primary key ((partition), batch, fragment)
) with clustering order by (batch asc, fragment asc);
