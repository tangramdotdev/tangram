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
