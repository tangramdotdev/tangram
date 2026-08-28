create table if not exists objects (
	bytes blob,
	id blob primary key,
	stored_at bigint
);

create table if not exists object_index_outbox (
	"batch" blob,
	fragment bigint,
	partition bigint,
	payload blob,
	primary key ((partition), "batch", fragment)
) with clustering order by ("batch" asc, fragment asc);

create table if not exists object_archive_outbox (
	id blob,
	partition bigint,
	stored_at bigint,
	primary key ((partition), stored_at, id)
) with clustering order by (stored_at asc, id asc);

create table if not exists logs (
	process blob,
	kind tinyint,
	position bigint,
	bytes blob,
	combined_position bigint,
	length int,
	stream tinyint,
	stream_position bigint,
	"timestamp" bigint,
	primary key ((process), kind, position)
) with clustering order by (kind asc, position asc);
