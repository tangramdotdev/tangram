create table if not exists objects (
	bytes blob,
	id blob,
	put blob,
	primary key ((id), put)
) with clustering order by (put desc)
	and tombstone_gc = {'mode': 'repair'};

create table if not exists object_cache (
	cache blob,
	object blob,
	partition bigint,
	put blob,
	primary key ((partition), cache)
) with clustering order by (cache asc)
	and tombstone_gc = {'mode': 'repair'};

create table if not exists archive_queue (
	indexer blob,
	sequence bigint,
	object blob,
	put blob,
	primary key ((indexer), sequence)
) with clustering order by (sequence asc)
	and tombstone_gc = {'mode': 'repair'};

create table if not exists index_queue (
	indexer blob,
	sequence bigint,
	"batch" blob,
	fragment bigint,
	fragments bigint,
	payload blob,
	primary key ((indexer), sequence)
) with clustering order by (sequence asc)
	and tombstone_gc = {'mode': 'repair'};

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
) with clustering order by (kind asc, position asc)
	and tombstone_gc = {'mode': 'repair'};
