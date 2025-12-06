create or replace procedure handle_queue_reference_count_cache_entry(
	inout n int8
)
language plpgsql
as $$
declare
	cache_entry_ids bytea[];
	dummy_count int8;
begin
	with dequeued as (
		delete from cache_entry_queue
		where id in (
			select id
			from cache_entry_queue
			order by id
			limit n
			for update skip locked
		)
		returning cache_entry
	)
	select coalesce(array_agg(cache_entry), '{}') into cache_entry_ids
	from dequeued;

	with locked as (
		select cache_entries.id
		from cache_entries
		where cache_entries.id = any(cache_entry_ids)
		order by cache_entries.id
		for update
	)
	select count(*) into dummy_count from locked;

	update cache_entries
	set
		reference_count = (
			select count(*)
			from objects
			where cache_entry = cache_entries.id
		),
		reference_count_transaction_id = (
			select id from transaction_id
		)
	where id = any(cache_entry_ids);

	n := n - coalesce(array_length(cache_entry_ids, 1), 0);
end;
$$;

create or replace procedure handle_queue_reference_count_object(
	inout n int8
)
language plpgsql
as $$
declare
	dequeued_objects bytea[];
	dummy_count int8;
begin
	with dequeued as (
		delete from object_queue
		where id in (
			select id
			from object_queue
			where kind = 0
			order by id
			limit n
			for update skip locked
		)
		returning object
	)
	select coalesce(array_agg(object), '{}') into dequeued_objects
	from dequeued;

	with locked as (
		select objects.id
		from objects
		where objects.id = any(dequeued_objects)
		order by objects.id
		for update
	)
	select count(*) into dummy_count from locked;

	update objects
	set
		reference_count = ref_counts.new_count,
		reference_count_transaction_id = (select id from transaction_id)
	from (
		select
			obj.id,
			coalesce(children_count.cnt, 0) +
			coalesce(process_count.cnt, 0) +
			coalesce(tag_count.cnt, 0) as new_count
		from unnest(dequeued_objects) as obj (id)
		left join (
			select child, count(*) as cnt
			from object_children
			where child = any(dequeued_objects)
			group by child
		) children_count on children_count.child = obj.id
		left join (
			select object, count(*) as cnt
			from process_objects
			where object = any(dequeued_objects)
			group by object
		) process_count on process_count.object = obj.id
		left join (
			select item, count(*) as cnt
			from tags
			where item = any(dequeued_objects)
			group by item
		) tag_count on tag_count.item = obj.id
	) ref_counts
	where objects.id = ref_counts.id;

	with locked as (
		select objects.id
		from objects
		where objects.id in (
			select child
			from object_children
			join objects parent_object on parent_object.id = object_children.object
			join objects child_object on child_object.id = object_children.child
			where object_children.object = any(dequeued_objects)
			and child_object.reference_count is not null
			and child_object.reference_count_transaction_id < parent_object.transaction_id
		)
		order by objects.id
		for update
	)
	select count(*) into dummy_count from locked;

	with valid_increments as (
		select
			child,
			count(*) as increment_amount
		from object_children
		join objects parent_object on parent_object.id = object_children.object
		join objects child_object on child_object.id = object_children.child
		where object_children.object = any(dequeued_objects)
		and child_object.reference_count is not null
		and child_object.reference_count_transaction_id < parent_object.transaction_id
		group by child
	)
	update objects
	set reference_count = reference_count + valid_increments.increment_amount
	from valid_increments
	where objects.id = valid_increments.child;

	with locked as (
		select cache_entries.id
		from cache_entries
		where cache_entries.id in (
			select objects.cache_entry
			from objects
			join cache_entries on cache_entries.id = objects.cache_entry
			where objects.id = any(dequeued_objects)
			and objects.cache_entry is not null
			and cache_entries.reference_count is not null
			and cache_entries.reference_count_transaction_id < objects.transaction_id
		)
		order by cache_entries.id
		for update
	)
	select count(*) into dummy_count from locked;

	with valid_increments as (
		select
			objects.cache_entry,
			count(*) as increment_amount
		from objects
		join cache_entries on cache_entries.id = objects.cache_entry
		where objects.id = any(dequeued_objects)
		and objects.cache_entry is not null
		and cache_entries.reference_count is not null
		and cache_entries.reference_count_transaction_id < objects.transaction_id
		group by objects.cache_entry
	)
	update cache_entries
	set reference_count = reference_count + valid_increments.increment_amount
	from valid_increments
	where cache_entries.id = valid_increments.cache_entry;

	n := n - coalesce(array_length(dequeued_objects, 1), 0);
end;
$$;

create or replace procedure handle_queue_reference_count_process(
	inout n int8
)
language plpgsql
as $$
declare
	dequeued_processes bytea[];
	dummy_count int8;
begin
	with dequeued as (
		delete from process_queue
		where id in (
			select id
			from process_queue
			where kind = 0
			order by id
			limit n
			for update skip locked
		)
		returning process
	)
	select coalesce(array_agg(process), '{}') into dequeued_processes
	from dequeued;

	with locked as (
		select processes.id
		from processes
		where processes.id = any(dequeued_processes)
		order by processes.id
		for update
	)
	select count(*) into dummy_count from locked;

	update processes
	set
		reference_count = ref_counts.new_count,
		reference_count_transaction_id = (select id from transaction_id)
	from (
		select
			processes.id,
			coalesce(children_count.cnt, 0) +
			coalesce(tag_count.cnt, 0) as new_count
		from unnest(dequeued_processes) as processes(id)
		left join (
			select child, count(*) as cnt
			from process_children
			where child = any(dequeued_processes)
			group by child
		) children_count on children_count.child = processes.id
		left join (
			select item, count(*) as cnt
			from tags
			where item = any(dequeued_processes)
			group by item
		) tag_count on tag_count.item = processes.id
	) ref_counts
	where processes.id = ref_counts.id;

	with locked as (
		select processes.id
		from processes
		where processes.id in (
			select child
			from process_children
			join processes parent_process on parent_process.id = process_children.process
			join processes child_process on child_process.id = process_children.child
			where process_children.process = any(dequeued_processes)
			and child_process.reference_count is not null
			and child_process.reference_count_transaction_id < parent_process.transaction_id
		)
		order by processes.id
		for update
	)
	select count(*) into dummy_count from locked;

	with valid_increments as (
			select
				child,
				count(*) as increment_amount
			from process_children
			join processes parent_process on parent_process.id = process_children.process
			join processes child_process on child_process.id = process_children.child
			where process_children.process = any(dequeued_processes)
			and child_process.reference_count is not null
			and child_process.reference_count_transaction_id < parent_process.transaction_id
			group by process_children.child
	)
	update processes
	set reference_count = reference_count + valid_increments.increment_amount
	from valid_increments
	where processes.id = valid_increments.child;

	with locked as (
		select objects.id
		from objects
		where objects.id in (
			select process_objects.object
			from process_objects
			join processes parent_process on parent_process.id = process_objects.process
			join objects on objects.id = process_objects.object
			where process_objects.process = any(dequeued_processes)
			and objects.reference_count is not null
			and objects.reference_count_transaction_id < parent_process.transaction_id
		)
		order by objects.id
		for update
	)
	select count(*) into dummy_count from locked;

	with valid_increments as (
			select
				process_objects.object,
				count(*) as increment_amount
			from process_objects
			join processes parent_process on parent_process.id = process_objects.process
			join objects on objects.id = process_objects.object
			where process_objects.process = any(dequeued_processes)
			and objects.reference_count is not null
			and objects.reference_count_transaction_id < parent_process.transaction_id
			group by process_objects.object
	)
	update objects
	set reference_count = reference_count + valid_increments.increment_amount
	from valid_increments
	where objects.id = valid_increments.object;

	n := n - coalesce(array_length(dequeued_processes, 1), 0);
end;
$$;
