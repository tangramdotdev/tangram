create table transaction_id (
	id int8
);

insert into transaction_id (id) select 0 where not exists (select 1 from transaction_id);

create table cache_entries (
	id bytea primary key,
	reference_count int8,
	reference_count_transaction_id int8,
	touched_at int8
);

create index cache_entries_reference_count_zero_index on cache_entries (touched_at) where reference_count = 0;

create table cache_entry_queue (
	id serial primary key,
	cache_entry bytea not null,
	transaction_id int8 not null
);

create index cache_entry_queue_transaction_id_index on cache_entry_queue (transaction_id);

create or replace procedure put_cache_entries(
		cache_entry_ids bytea[],
		cache_entry_touched_ats int8[]
)
language plpgsql
as $$
declare
	inserted_ids bytea[];
	dummy_count int8;
begin
	if array_length(cache_entry_ids, 1) > 0 then
		with locked as (
			select cache_entries.id
			from cache_entries
			where cache_entries.id = any(cache_entry_ids)
			order by cache_entries.id
			for update
		)
		select count(*) into dummy_count from locked;

		with upsert as (
			insert into cache_entries (id, touched_at)
			select id, touched_at
			from unnest(cache_entry_ids, cache_entry_touched_ats) as t (id, touched_at)
			on conflict (id) do update set touched_at = excluded.touched_at
			returning id, xmax = 0 as was_inserted
		)
		select coalesce(array_agg(id), '{}') into inserted_ids
		from upsert
		where was_inserted;

		insert into cache_entry_queue (cache_entry, transaction_id)
		select id, (select id from transaction_id)
		from unnest(inserted_ids) as t(id);
	end if;
end;
$$;

create table objects (
	id bytea primary key,
	cache_entry bytea,
	complete boolean not null default false,
	count int8,
	depth int8,
	transaction_id int8 not null,
	reference_count int8,
	reference_count_transaction_id int8,
	size int8 not null,
	touched_at int8,
	weight int8
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

create index object_queue_transaction_id_index on object_queue (transaction_id);

create index object_queue_kind_index on object_queue (kind, id);

create table processes (
	id bytea primary key,
	children_complete boolean not null default false,
	children_count int8,
	children_commands_complete boolean not null default false,
	children_commands_count int8,
	children_commands_depth int8,
	children_commands_weight int8,
	children_outputs_complete boolean not null default false,
	children_outputs_count int8,
	children_outputs_depth int8,
	children_outputs_weight int8,
	command_complete boolean not null default false,
	command_count int8,
	command_depth int8,
	command_weight int8,
	output_complete boolean not null default false,
	output_count int8,
	output_depth int8,
	output_weight int8,
	reference_count int8,
	reference_count_transaction_id int8,
	touched_at int8,
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

create index process_queue_transaction_id_index on process_queue (transaction_id);

create index process_queue_kind_index on process_queue (kind, id);

create table tags (
	tag text primary key,
	item bytea not null
);

create index tags_item_index on tags (item);
