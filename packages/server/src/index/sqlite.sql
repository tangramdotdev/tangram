create table transaction_id (
	id integer
);

insert into transaction_id (id) values (0);

create table cache_entries (
	id blob primary key,
	reference_count integer,
	reference_count_transaction_id integer,
	touched_at integer
);

create index cache_entries_reference_count_zero_index on cache_entries (touched_at) where reference_count = 0;

create table cache_entry_queue (
	id integer primary key autoincrement,
	cache_entry blob not null,
	transaction_id integer not null
);

create index cache_entry_queue_transaction_id_index on cache_entry_queue (transaction_id);

create table objects (
	id blob primary key,
	cache_entry blob,
	node_size integer not null,
	reference_count integer,
	reference_count_transaction_id integer,
	subtree_count integer,
	subtree_depth integer,
	subtree_size integer,
	subtree_stored integer not null default 0,
	touched_at integer,
	transaction_id integer not null
);

create index objects_reference_count_zero_index on objects (touched_at) where reference_count = 0;

create index objects_cache_entry_index on objects (cache_entry) where cache_entry is not null;

create table object_children (
	object blob not null,
	child blob not null
);

create unique index object_children_index on object_children (object, child);

create index object_children_child_index on object_children (child);

create table object_queue (
	id integer primary key autoincrement,
	kind integer not null,
	object blob not null,
	transaction_id integer not null
);

create index object_queue_transaction_id_index on object_queue (transaction_id);

create index object_queue_kind_index on object_queue (kind, id);

create table processes (
	id blob primary key,
	node_command_count integer,
	node_command_depth integer,
	node_command_size integer,
	node_command_stored integer not null default 0,
	node_output_count integer,
	node_output_depth integer,
	node_output_size integer,
	node_output_stored integer not null default 0,
	reference_count integer,
	reference_count_transaction_id integer,
	subtree_command_count integer,
	subtree_command_depth integer,
	subtree_command_size integer,
	subtree_command_stored integer not null default 0,
	subtree_output_count integer,
	subtree_output_depth integer,
	subtree_output_size integer,
	subtree_output_stored integer not null default 0,
	subtree_count integer,
	subtree_stored integer not null default 0,
	touched_at integer,
	transaction_id integer not null
);

create index processes_reference_count_zero_index on processes (touched_at) where reference_count = 0;

create table process_children (
	process blob not null,
	child blob not null,
	position integer not null
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_index on process_children (process, position);

create index process_children_child_process_index on process_children (child, process);

create table process_objects (
	process blob not null,
	object blob not null,
	kind integer not null
);

create unique index process_objects_index on process_objects (process, object, kind);

create index process_objects_object_index on process_objects (object);

create table process_queue (
	id integer primary key autoincrement,
	process blob not null,
	kind integer not null,
	transaction_id integer not null
);

create index process_queue_transaction_id_index on process_queue (transaction_id);

create index process_queue_kind_index on process_queue (kind, id);

create table tags (
	tag text primary key,
	item blob not null
);

create index tags_item_index on tags (item);
