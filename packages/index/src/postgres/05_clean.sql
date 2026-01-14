create or replace procedure clean(
	max_touched_at int8,
	batch_size int8,
	out deleted_bytes int8,
	out deleted_cache_entries bytea[],
	out deleted_objects bytea[],
	out deleted_processes bytea[]
)
language plpgsql
as $$
declare
	n int8;
begin
	n := batch_size;
	deleted_bytes := 0;

	if n > 0 then
		call clean_cache_entries(max_touched_at, n, deleted_cache_entries);
		n := n - coalesce(array_length(deleted_cache_entries, 1), 0);
	end if;

	if n > 0 then
		call clean_objects(max_touched_at, n, deleted_bytes, deleted_objects);
		n := n - coalesce(array_length(deleted_objects, 1), 0);
	end if;

	if n > 0 then
		call clean_processes(max_touched_at, n, deleted_processes);
		n := n - coalesce(array_length(deleted_processes, 1), 0);
	end if;

	n := batch_size - n;
end;
$$;

create or replace procedure clean_cache_entries(
	max_touched_at int8,
	n int8,
	out deleted_cache_entries bytea[]
)
language plpgsql
as $$
declare
	locked_count int8;
begin
	with candidates as (
		select id from cache_entries
		where reference_count = 0 and touched_at <= max_touched_at
		order by id
		limit n
	)
	select coalesce(array_agg(id), '{}') into deleted_cache_entries from candidates;

	with locked as (
		select cache_entries.id
		from cache_entries
		where cache_entries.id = any(deleted_cache_entries)
		order by cache_entries.id
		for update
	)
	select count(*) into locked_count from locked;

	with updated as (
		update cache_entries
		set reference_count = (
			select count(*) from objects where cache_entry = cache_entries.id
		),
		reference_count_transaction_id = (select id from transaction_id)
		where id = any(deleted_cache_entries)
		returning id, reference_count
	)
	select coalesce(array_agg(id), '{}') into deleted_cache_entries
	from updated
	where reference_count = 0;

	delete from cache_entries where id = any(deleted_cache_entries);
end;
$$;

create or replace procedure clean_objects(
	max_touched_at int8,
	n int8,
	inout deleted_bytes int8,
	out deleted_objects bytea[]
)
language plpgsql
as $$
declare
	locked_count int8;
begin
	with candidates as (
		select id from objects
		where reference_count = 0 and touched_at <= max_touched_at
		order by id
		limit n
	)
	select coalesce(array_agg(id), '{}') into deleted_objects from candidates;

	with locked as (
		select objects.id
		from objects
		where objects.id = any(deleted_objects)
		order by objects.id
		for update
	)
	select count(*) into locked_count from locked;

	with updated as (
		update objects
		set reference_count = (
			(select count(*) from object_children where child = objects.id) +
			(select count(*) from process_objects where object = objects.id) +
			(select count(*) from tags where item = objects.id)
		),
		reference_count_transaction_id = (select id from transaction_id)
		where id = any(deleted_objects)
		returning id, reference_count, node_size
	)
	select coalesce(array_agg(id), '{}'), deleted_bytes + coalesce(sum(node_size), 0)
	into deleted_objects, deleted_bytes
	from updated
	where reference_count = 0;

	with locked as (
		select objects.id
		from objects
		where objects.id in (
			select distinct child
			from object_children
			where object = any(deleted_objects)
		)
		order by objects.id
		for update
	)
	select count(*) into locked_count from locked;

	update objects
	set reference_count = reference_count - 1
	from (
		select distinct child
		from object_children
		where object = any(deleted_objects)
	) as children
	where objects.id = children.child;

	with locked as (
		select cache_entries.id
		from cache_entries
		where cache_entries.id in (
			select distinct cache_entry
			from objects
			where id = any(deleted_objects)
			and cache_entry is not null
		)
		order by cache_entries.id
		for update
	)
	select count(*) into locked_count from locked;

	update cache_entries
	set reference_count = reference_count - 1
	from (
		select distinct cache_entry
		from objects
		where id = any(deleted_objects)
		and cache_entry is not null
	) as entries
	where cache_entries.id = entries.cache_entry;

	delete from object_children where object = any(deleted_objects);
	delete from objects where id = any(deleted_objects);
end;
$$;

create or replace procedure clean_processes(
	max_touched_at int8,
	n int8,
	out deleted_processes bytea[]
)
language plpgsql
as $$
declare
	locked_count int8;
begin
	with candidates as (
		select id from processes
		where reference_count = 0 and touched_at <= max_touched_at
		order by id
		limit n
	)
	select coalesce(array_agg(id), '{}') into deleted_processes from candidates;

	with locked as (
		select processes.id
		from processes
		where processes.id = any(deleted_processes)
		order by processes.id
		for update
	)
	select count(*) into locked_count from locked;

	with updated as (
		update processes
		set reference_count = (
			(select count(*) from process_children where child = processes.id) +
			(select count(*) from tags where item = processes.id)
		),
		reference_count_transaction_id = (select id from transaction_id)
		where id = any(deleted_processes)
		returning id, reference_count
	)
	select coalesce(array_agg(id), '{}') into deleted_processes
	from updated
	where reference_count = 0;

	with locked as (
		select processes.id
		from processes
		where processes.id in (
			select distinct child
			from process_children
			where process = any(deleted_processes)
		)
		order by processes.id
		for update
	)
	select count(*) into locked_count from locked;

	update processes
	set reference_count = reference_count - 1
	from (
		select distinct child
		from process_children
		where process = any(deleted_processes)
	) as children
	where processes.id = children.child;

	with locked as (
		select objects.id
		from objects
		where objects.id in (
			select distinct object
			from process_objects
			where process = any(deleted_processes)
		)
		order by objects.id
		for update
	)
	select count(*) into locked_count from locked;

	update objects
	set reference_count = reference_count - 1
	from (
		select distinct object
		from process_objects
		where process = any(deleted_processes)
	) as process_objs
	where objects.id = process_objs.object;

	delete from process_children where process = any(deleted_processes);
	delete from process_objects where process = any(deleted_processes);
	delete from processes where id = any(deleted_processes);
end;
$$;
