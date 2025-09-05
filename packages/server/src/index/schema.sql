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

create trigger cache_entry_insert_trigger
after insert on cache_entries
begin
	insert into cache_entry_queue (cache_entry, transaction_id) values (new.id, (select id from transaction_id));
end;

create table objects (
	id blob primary key,
	cache_entry blob,
	complete integer not null default 0,
	count integer,
	depth integer,
	transaction_id integer not null,
	reference_count integer,
	reference_count_transaction_id integer,
	size integer not null,
	touched_at integer,
	weight integer
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

create trigger object_queue_trigger
after insert on objects
begin
	insert into object_queue (object, kind, transaction_id) values (new.id, 0, (select id from transaction_id));
	insert into object_queue (object, kind, transaction_id) values (new.id, 1, (select id from transaction_id));
end;

create table processes (
	id blob primary key,
	children_complete integer not null default 0,
	children_count integer,
	commands_complete integer not null default 0,
	commands_count integer,
	commands_depth integer,
	commands_weight integer,
	outputs_complete integer not null default 0,
	outputs_count integer,
	outputs_depth integer,
	outputs_weight integer,
	reference_count integer,
	reference_count_transaction_id integer,
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
	kind text not null
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

create trigger process_queue_trigger
after insert on processes
begin
	insert into process_queue (process, kind, transaction_id) values (new.id, 0, (select id from transaction_id));
	insert into process_queue (process, kind, transaction_id) values (new.id, 1, (select id from transaction_id));
	insert into process_queue (process, kind, transaction_id) values (new.id, 2, (select id from transaction_id));
	insert into process_queue (process, kind, transaction_id) values (new.id, 3, (select id from transaction_id));
end;

create table tags (
	tag text primary key,
	item blob not null
);

create index tags_item_index on tags (item);
