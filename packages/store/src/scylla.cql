create table if not exists objects (
	bytes blob,
	id blob,
	put blob,
	primary key ((id), put)
) with clustering order by (put desc);

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
	put blob,
	primary key ((partition), put)
) with clustering order by (put asc);

create table if not exists object_cache (
	cache blob,
	id blob,
	partition bigint,
	put blob,
	primary key ((partition), cache)
) with clustering order by (cache asc);

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
