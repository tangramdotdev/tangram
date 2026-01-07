create table transaction_id (
	id int8
);

insert into transaction_id (id) select 0 where not exists (select 1 from transaction_id);

create table cache_entries (
	id bytea primary key,
	reference_count int8,
	reference_count_transaction_id int8,
	touched_at int8 not null
);

create index cache_entries_reference_count_zero_index on cache_entries (touched_at) where reference_count = 0;

create table cache_entry_queue (
	id serial primary key,
	cache_entry bytea not null,
	transaction_id int8 not null
);

create index cache_entry_queue_transaction_id_index on cache_entry_queue (transaction_id);

create table objects (
	id bytea primary key,
	cache_entry bytea,
	node_size int8 not null,
	node_solvable boolean not null,
	node_solved boolean not null,
	reference_count int8,
	reference_count_transaction_id int8,
	subtree_count int8,
	subtree_depth int8,
	subtree_size int8,
	subtree_solvable boolean,
	subtree_solved boolean,
	subtree_stored boolean not null default false,
	touched_at int8 not null,
	transaction_id int8 not null
);

create index objects_reference_count_zero_index on objects (touched_at) where reference_count = 0;

create index objects_cache_entry_index on objects (cache_entry) where cache_entry is not null;

create table object_children (
	object bytea not null,
	child bytea not null
);

create unique index object_children_index on object_children (object, child);

create index object_children_child_index on object_children (child);

create table object_queue (
	id serial primary key,
	kind int8 not null,
	object bytea not null,
	transaction_id int8 not null
);

create unique index object_queue_object_kind_index on object_queue (object, kind);

create index object_queue_transaction_id_index on object_queue (transaction_id);

create index object_queue_kind_index on object_queue (kind, id);

create table processes (
	id bytea primary key,
	node_command_count int8,
	node_command_depth int8,
	node_command_size int8,
	node_command_stored boolean not null default false,
	node_error_count int8,
	node_error_depth int8,
	node_error_size int8,
	node_error_stored boolean not null default false,
	node_log_count int8,
	node_log_depth int8,
	node_log_size int8,
	node_log_stored boolean not null default false,
	node_output_count int8,
	node_output_depth int8,
	node_output_size int8,
	node_output_stored boolean not null default false,
	reference_count int8,
	reference_count_transaction_id int8,
	subtree_command_count int8,
	subtree_command_depth int8,
	subtree_command_size int8,
	subtree_command_stored boolean not null default false,
	subtree_error_count int8,
	subtree_error_depth int8,
	subtree_error_size int8,
	subtree_error_stored boolean not null default false,
	subtree_log_count int8,
	subtree_log_depth int8,
	subtree_log_size int8,
	subtree_log_stored boolean not null default false,
	subtree_output_count int8,
	subtree_output_depth int8,
	subtree_output_size int8,
	subtree_output_stored boolean not null default false,
	subtree_count int8,
	subtree_stored boolean not null default false,
	touched_at int8 not null,
	transaction_id int8 not null
);

create index processes_reference_count_zero_index on processes (touched_at) where reference_count = 0;

create table process_children (
	process bytea not null,
	child bytea not null,
	position int8 not null
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_index on process_children (process, position);

create index process_children_child_process_index on process_children (child, process);

create table process_objects (
	process bytea not null,
	object bytea not null,
	kind int8 not null
);

create unique index process_objects_index on process_objects (process, object, kind);

create index process_objects_object_index on process_objects (object);

create table process_queue (
	id serial primary key,
	process bytea not null,
	kind int8 not null,
	transaction_id int8 not null
);

create unique index process_queue_process_kind_index on process_queue (process, kind);

create index process_queue_transaction_id_index on process_queue (transaction_id);

create index process_queue_kind_index on process_queue (kind, id);

create table tags (
	tag text primary key,
	item bytea not null
);

create index tags_item_index on tags (item);
