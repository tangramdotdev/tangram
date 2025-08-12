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

create trigger objects_insert_complete_trigger
after insert on objects
when new.complete = 0 and new.incomplete_children = 0
begin
	update objects
	set
		complete = updates.complete,
		count = updates.count,
		depth = updates.depth,
		weight = updates.weight
	from (
		select
			objects.id,
			coalesce(min(child_objects.complete), 1) as complete,
			1 + coalesce(sum(child_objects.count), 0) as count,
			1 + coalesce(max(child_objects.depth), 0) as depth,
			objects.size + coalesce(sum(child_objects.weight), 0) as weight
		from objects
		left join object_children on object_children.object = objects.id
		left join objects as child_objects on child_objects.id = object_children.child
		where objects.id = new.id
		group by objects.id, objects.size
	) as updates
	where objects.id = updates.id;
end;

create trigger objects_update_complete_trigger
after update of complete, incomplete_children on objects
when new.complete = 0 and new.incomplete_children = 0
begin
	update objects
	set
		complete = updates.complete,
		count = updates.count,
		depth = updates.depth,
		weight = updates.weight
	from (
		select
			objects.id,
			coalesce(min(child_objects.complete), 1) as complete,
			1 + coalesce(sum(child_objects.count), 0) as count,
			1 + coalesce(max(child_objects.depth), 0) as depth,
			objects.size + coalesce(sum(child_objects.weight), 0) as weight
		from objects
		left join object_children on object_children.object = objects.id
		left join objects as child_objects on child_objects.id = object_children.child
		where objects.id = new.id
		group by objects.id, objects.size
	) as updates
	where objects.id = updates.id;
end;

create trigger objects_insert_incomplete_children_trigger
after insert on objects
for each row
when (new.incomplete_children is null)
begin
	update objects
	set incomplete_children = (
		select count(*)
		from object_children
		left join objects child_objects on child_objects.id = object_children.child
		where object_children.object = new.id and (child_objects.complete is null or child_objects.complete = 0)
	)
	where id = new.id;
end;

create trigger objects_insert_reference_count_trigger
after insert on objects
for each row
when (new.reference_count is null)
begin
	update objects
	set reference_count = (
		(select count(*) from object_children where child = new.id) +
		(select count(*) from process_objects where object = new.id) +
		(select count(*) from tags where item = new.id)
	)
	where id = new.id;
end;

create trigger objects_update_incomplete_children_on_insert_trigger
after insert on objects
for each row
when new.complete = 1
begin
	update objects
	set incomplete_children = incomplete_children - 1
	where id in (
		select object
		from object_children
		where child = new.id
	);
end;

create trigger objects_update_incomplete_children_on_update_trigger
after update of complete on objects
for each row
when (old.complete = 0 and new.complete = 1)
begin
	update objects
	set incomplete_children = incomplete_children - 1
	where id in (
		select object
		from object_children
		where child = new.id
	);
end;

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
	children_complete integer not null default 0,
	children_count integer,
	commands_complete integer not null default 0,
	commands_count integer,
	commands_depth integer,
	commands_weight integer,
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

create trigger processes_insert_complete_trigger
after insert on processes
when new.children_complete = 0 and new.incomplete_children = 0
begin
	update processes
	set
		children_complete = updates.children_complete,
		children_count = updates.children_count
	from (
		select
			processes.id,
			coalesce(min(child_processes.children_complete), 1) as children_complete,
			1 + coalesce(sum(child_processes.children_count), 0) as children_count
		from processes
		left join process_children on process_children.process = processes.id
		left join processes as child_processes on child_processes.id = process_children.child
		where processes.id = new.id
		group by processes.id
	) as updates
	where processes.id = updates.id;
end;

create trigger processes_update_complete_trigger
after update of children_complete, incomplete_children on processes
when new.children_complete = 0 and new.incomplete_children = 0
begin
	update processes
	set
		children_complete = updates.children_complete,
		children_count = updates.children_count
	from (
		select
			processes.id,
			coalesce(min(child_processes.children_complete), 1) as children_complete,
			1 + coalesce(sum(child_processes.children_count), 0) as children_count
		from processes
		left join process_children on process_children.process = processes.id
		left join processes as child_processes on child_processes.id = process_children.child
		where processes.id = new.id
		group by processes.id
	) as updates
	where processes.id = updates.id;
end;

create trigger processes_update_commands_complete_trigger
after update of complete, incomplete_commands on processes
when new.commands_complete = 0 and new.incomplete_commands = 0 and new.incomplete_children = 0
begin
	update processes
	set
		commands_complete = updates.commands_complete,
		commands_count = updates.commands_count,
		commands_depth = updates.commands_depth,
		commands_weight = updates.commands_weight
	from (
		select
			processes.id,
			min(coalesce(min(command_objects.complete), 1), coalesce(min(child_processes.commands_complete), 1)) as commands_complete,
			coalesce(sum(command_objects.count), 0) + coalesce(sum(child_processes.commands_count), 0) as commands_count,
			max(coalesce(max(command_objects.depth), 0), coalesce(max(child_processes.commands_depth), 0)) as commands_depth,
			coalesce(sum(command_objects.weight), 0) + coalesce(sum(child_processes.commands_weight), 0) as commands_weight
		from processes
		left join process_objects process_objects_commands on process_objects_commands.process = processes.id and process_objects_commands.kind = 'command'
		left join objects command_objects on command_objects.id = process_objects_commands.object
		left join process_children process_children_commands on process_children_commands.process = processes.id
		left join processes child_processes on child_processes.id = process_children_commands.child
		where processes.id = new.id
		group by processes.id
	) as updates
	where processes.id = updates.id;
end;

create trigger processes_update_outputs_complete_trigger
after update of complete, incomplete_outputs on processes
when new.outputs_complete = 0 and new.incomplete_outputs = 0 and new.incomplete_children = 0
begin
	update processes
	set
		outputs_complete = updates.outputs_complete,
		outputs_count = updates.outputs_count,
		outputs_depth = updates.outputs_depth,
		outputs_weight = updates.outputs_weight
	from (
		select
			processes.id,
			min(coalesce(min(output_objects.complete), 1), coalesce(min(child_processes.outputs_complete), 1)) as outputs_complete,
			coalesce(sum(output_objects.count), 0) + coalesce(sum(child_processes.outputs_count), 0) as outputs_count,
			max(coalesce(max(output_objects.depth), 0), coalesce(max(child_processes.outputs_depth), 0)) as outputs_depth,
			coalesce(sum(output_objects.weight), 0) + coalesce(sum(child_processes.outputs_weight), 0) as outputs_weight
		from processes
		left join process_objects process_objects_outputs on process_objects_outputs.process = processes.id and process_objects_outputs.kind = 'output'
		left join objects output_objects on output_objects.id = process_objects_outputs.object
		left join process_children process_children_outputs on process_children_outputs.process = processes.id
		left join processes child_processes on child_processes.id = process_children_outputs.child
		where processes.id = new.id
		group by processes.id
	) as updates
	where processes.id = updates.id;
end;

create trigger processes_insert_incomplete_children_trigger
after insert on processes
when new.incomplete_children is null
begin
	update processes
	set incomplete_children = (
		select count(*)
		from process_children
		left join processes child_processes on child_processes.id = process_children.child
		where process_children.process = new.id
		and (child_processes.children_complete is null or child_processes.children_complete = 0)
	)
	where id = new.id;
end;

create trigger children_processes_update_incomplete_children_trigger
after update of complete on processes
when old.complete = 0 and new.complete = 1
begin
	update processes
	set incomplete_children = incomplete_children - 1
	where id in (
		select process
		from process_children
		where process_children.child = new.id
	);
end;

create trigger processes_insert_incomplete_commands_trigger
after insert on processes
when new.incomplete_commands is null
begin
	update processes
	set incomplete_commands = (
		select count(*)
		from process_objects
		left join objects on objects.id = process_objects.object
		where process_objects.process = new.id
		and process_objects.kind = 'command'
		and (objects.complete is null or objects.complete = 0)
	)
	where id = new.id;
end;

create trigger processes_update_incomplete_commands_on_insert_trigger
after insert on objects
for each row
when new.complete = 1
begin
	update processes
	set incomplete_commands = incomplete_commands - 1
	where id in (
		select process
		from process_objects
		where process_objects.object = new.id and process_objects.kind = 'command'
	);
end;

create trigger processes_update_incomplete_commands_on_update_trigger
after update of complete on objects
for each row
when old.complete = 0 and new.complete = 1
begin
	update processes
	set incomplete_commands = incomplete_commands - 1
	where id in (
		select process
		from process_objects
		where process_objects.object = new.id and process_objects.kind = 'command'
	);
end;

create trigger processes_insert_incomplete_outputs_trigger
after insert on processes
when new.incomplete_outputs is null
begin
	update processes
	set incomplete_outputs = (
		select count(*)
		from process_objects
		left join objects on objects.id = process_objects.object
		where process_objects.process = new.id
		and process_objects.kind = 'output'
		and (objects.complete is null or objects.complete = 0)
	)
	where id = new.id;
end;

create trigger processes_update_incomplete_outputs_trigger
after update of complete on objects
for each row
when old.complete = 0 and new.complete = 1
begin
	update processes
	set incomplete_outputs = incomplete_outputs - 1
	where id in (
		select process
		from process_objects
		where process_objects.object = new.id and process_objects.kind = 'output'
	);
end;

create index processes_reference_count_zero_index on processes (touched_at) where reference_count = 0;

create trigger processes_insert_reference_count_trigger
after insert on processes
for each row
when (new.reference_count is null)
begin
	update processes
	set reference_count = (
		(select count(*) from process_children where child = new.id) +
		(select count(*) from tags where item = new.id)
	)
	where id = new.id;
end;

create trigger processes_insert_incomplete_children_commands_trigger
after insert on processes
when (new.incomplete_children_commands is null)
begin
	update processes
	set incomplete_children_commands = (
		select count(*)
		from process_children
		left join processes child_processes on child_processes.id = process_children.child
		where process_children.process = new.id
		and (child_processes.children_complete is null or child_processes.commands_complete = 0)
	)
	where id = new.id;
end;

create trigger processes_update_incomplete_children_commands_trigger
after update of commands_complete on processes
when (new.commands_complete = 1)
begin
	update processes
	set incomplete_children_commands = incomplete_children_commands - 1
	where id in (
		select process
		from process_children
		where process_children.child = new.id
	);
end;

create trigger processes_insert_incomplete_children_outputs_trigger
after insert on processes
when (new.incomplete_children_outputs is null)
begin
	update processes
	set incomplete_children_outputs = (
		select count(*)
		from process_children
		left join processes child_processes on child_processes.id = process_children.child
		where process_children.process = new.id
		and (child_processes.children_complete is null or child_processes.outputs_complete = 0)
	)
	where id = new.id;
end;

create trigger processes_update_incomplete_children_outputs_trigger
after update of outputs_complete on processes
when (new.outputs_complete = 1)
begin
	update processes
	set incomplete_children_outputs = incomplete_children_outputs - 1
	where id in (
		select process
		from process_children
		where process_children.child = new.id
	);
end;

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
	position integer not null
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

create index tags_item_index on tags (item);

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
