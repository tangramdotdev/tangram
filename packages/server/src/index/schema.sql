create table cache_entries (
	id text primary key,
	reference_count integer,
	touched_at integer
);

create index cache_entries_reference_count_zero_index on cache_entries (touched_at) where reference_count = 0;

create trigger cache_entries_insert_reference_count_trigger
after insert on cache_entries
for each row
when (new.reference_count is null)
begin
	update cache_entries
	set reference_count = (
		(select count(*) from objects where cache_reference = new.id) +
		(select count(*) from tags where item = new.id)
	)
	where id = new.id;
end;

create table objects (
	id text primary key,
	cache_reference text,
	complete integer not null default 0,
	count integer,
	depth integer,
	incomplete_children integer,
	reference_count integer,
	size integer not null,
	touched_at integer,
	weight integer
);

create index objects_reference_count_zero_index on objects (touched_at) where reference_count = 0;

create index objects_cache_entry_index on objects (cache_reference) where cache_reference is not null;

create trigger objects_insert_cache_reference_trigger
after insert on objects
for each row
begin
	update cache_entries
	set reference_count = reference_count + 1
	where id = new.cache_reference;
end;

create trigger objects_delete_trigger
after delete on objects
for each row
begin
	update cache_entries
	set reference_count = reference_count - 1
	where id = old.cache_reference;

	delete from object_children
	where object = old.id;
end;

create table object_children (
	object text not null,
	child text not null
);

create unique index object_children_index on object_children (object, child);

create index object_children_child_index on object_children (child);

create trigger object_children_insert_trigger
after insert on object_children
for each row
begin
	update objects
	set reference_count = objects.reference_count + 1
	where id = new.child;
end;

create trigger object_children_delete_trigger
after delete on object_children
for each row
begin
	update objects
	set reference_count = objects.reference_count - 1
	where id = old.child;
end;

create table processes (
	id text primary key,
	count integer,
	commands_complete integer not null default 0,
	commands_count integer,
	commands_depth integer,
	commands_weight integer,
	complete integer not null default 0,
	incomplete_children integer,
	incomplete_children_commands integer,
	incomplete_children_outputs integer,
	incomplete_commands integer,
	incomplete_outputs integer,
	outputs_complete integer not null default 0,
	outputs_count integer,
	outputs_depth integer,
	outputs_weight integer,
	reference_count integer,
	touched_at integer
);

create index processes_reference_count_zero_index on processes (touched_at) where reference_count = 0;

create trigger processes_delete_trigger
after delete on processes
for each row
begin
	delete from process_children
	where process = old.id;

	delete from process_objects
	where process = old.id;
end;

create table process_children (
	process text not null,
	child text not null,
	position integer not null,
	path text,
	tag text
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_index on process_children (process, position);

create index process_children_child_process_index on process_children (child);

create trigger process_children_insert_trigger
after insert on process_children
for each row
begin
	update processes
	set reference_count = processes.reference_count + 1
	where id = new.child;
end;

create trigger process_children_delete_trigger
after delete on process_children
for each row
begin
	update processes
	set reference_count = processes.reference_count - 1
	where id = old.child;
end;

create table process_objects (
	process text not null,
	object text not null,
	kind text not null
);

create unique index process_objects_index on process_objects (process, object, kind);

create index process_objects_object_index on process_objects (object);

create trigger process_objects_insert_trigger
after insert on process_objects
begin
	update objects
	set reference_count = reference_count + 1
	where id = new.object;
end;

create trigger process_objects_delete_trigger
after delete on process_objects
begin
	update objects
	set reference_count = reference_count - 1
	where id = old.object;
end;

create table tags (
	tag text primary key,
	item text not null
);

create trigger tags_insert_trigger
after insert on tags
for each row
begin
	update cache_entries set reference_count = reference_count + 1
	where id = new.item;

	update objects set reference_count = reference_count + 1
	where id = new.item;

	update processes set reference_count = reference_count + 1
	where id = new.item;
end;

create trigger tags_delete_trigger
after delete on tags
for each row
begin
	update cache_entries set reference_count = reference_count - 1
	where id = old.item;

	update objects set reference_count = reference_count - 1
	where id = old.item;

	update processes set reference_count = reference_count - 1
	where id = old.item;
end;
