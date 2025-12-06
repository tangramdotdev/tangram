create or replace procedure handle_messages(
	cache_entry_ids bytea[],
	cache_entry_touched_ats int8[],

	object_ids bytea[],
	object_cache_entries bytea[],
	object_sizes int8[],
	object_touched_ats int8[],
	object_counts int8[],
	object_depths int8[],
	object_weights int8[],
	object_completes bool[],
	object_children bytea[],
	object_parent_indices int8[],

	touch_object_touched_ats int8[],
	touch_object_ids bytea[],

	process_ids bytea[],
	process_touched_ats int8[],
	process_children_completes bool[],
	process_children_counts int8[],
	process_children_commands_completes bool[],
	process_children_commands_counts int8[],
	process_children_commands_depths int8[],
	process_children_commands_weights int8[],
	process_children_outputs_completes bool[],
	process_children_outputs_counts int8[],
	process_children_outputs_depths int8[],
	process_children_outputs_weights int8[],
	process_command_completes bool[],
	process_command_counts int8[],
	process_command_depths int8[],
	process_command_weights int8[],
	process_output_completes bool[],
	process_output_counts int8[],
	process_output_depths int8[],
	process_output_weights int8[],
	process_children bytea[],
	process_child_process_indices int8[],
	process_child_positions int8[],
	process_objects bytea[],
	process_object_kinds int8[],
	process_object_process_indices int8[],

	touch_process_touched_ats int8[],
	touch_process_ids bytea[],

	put_tag_tags text[],
	put_tag_items bytea[],

	delete_tags text[]
)
language plpgsql
as $$
begin
	call put_cache_entries(cache_entry_ids, cache_entry_touched_ats);

	call put_objects(
		object_ids,
		object_cache_entries,
		object_sizes,
		object_touched_ats,
		object_counts,
		object_depths,
		object_weights,
		object_completes,
		object_children,
		object_parent_indices
	);

	call touch_objects(touch_object_touched_ats, touch_object_ids);

	call put_processes(
		process_ids,
		process_touched_ats,
		process_children_completes,
		process_children_counts,
		process_children_commands_completes,
		process_children_commands_counts,
		process_children_commands_depths,
		process_children_commands_weights,
		process_children_outputs_completes,
		process_children_outputs_counts,
		process_children_outputs_depths,
		process_children_outputs_weights,
		process_command_completes,
		process_command_counts,
		process_command_depths,
		process_command_weights,
		process_output_completes,
		process_output_counts,
		process_output_depths,
		process_output_weights,
		process_children,
		process_child_process_indices,
		process_child_positions,
		process_objects,
		process_object_kinds,
		process_object_process_indices
	);

	call touch_processes(touch_process_touched_ats, touch_process_ids);

	call put_tags(put_tag_tags, put_tag_items);

	call delete_tags(delete_tags);

	if
		coalesce(array_length(cache_entry_ids, 1), 0) +
		coalesce(array_length(object_ids, 1), 0) +
		coalesce(array_length(touch_object_ids, 1), 0) +
		coalesce(array_length(process_ids, 1), 0) +
		coalesce(array_length(touch_process_ids, 1), 0) +
		coalesce(array_length(put_tag_tags, 1), 0) +
		coalesce(array_length(delete_tags, 1), 0)
		> 0
	then
		update transaction_id set id = id + 1;
	end if;
end;
$$;

create or replace procedure put_objects(
	ids bytea[],
	cache_entries bytea[],
	sizes int8[],
	touched_ats int8[],
	counts int8[],
	depths int8[],
	weights int8[],
	completes boolean[],
	children bytea[],
	parent_indices int8[]
)
language plpgsql
as $$
declare
	inserted_ids bytea[];
	dummy_count int8;
begin
	with locked as (
		select objects.id
		from objects
		where objects.id = any(ids)
		order by objects.id
		for update
	)
	select count(*) into dummy_count from locked;

	with upsert as (
		insert into objects (id, cache_entry, size, touched_at, count, depth, weight, complete, transaction_id)
		select id, cache_entry, size, touched_at, count, depth, weight, complete, (select id from transaction_id)
		from unnest(ids, cache_entries, sizes, touched_ats, counts, depths, weights, completes)
			as t (id, cache_entry, size, touched_at, count, depth, weight, complete)
		on conflict (id) do update set
			complete = excluded.complete or objects.complete,
			count = coalesce(excluded.count, objects.count),
			depth = coalesce(excluded.depth, objects.depth),
			weight = coalesce(excluded.weight, objects.weight),
			touched_at = excluded.touched_at
		returning id, xmax = 0 as was_inserted
	)
	select coalesce(array_agg(id), '{}') into inserted_ids
	from upsert
	where was_inserted;

	insert into object_queue (object, kind, transaction_id)
	select id, kind, (select id from transaction_id)
	from unnest(inserted_ids) as t(id)
	cross join (values (0), (1)) as kinds(kind);

	insert into object_children (object, child)
	select ids[object_index], child
	from unnest(parent_indices, children) as t (object_index, child)
	on conflict (object, child) do nothing;
end;
$$;

create or replace procedure put_processes(
	process_ids bytea[],
	touched_ats int8[],
	children_completes boolean[],
	children_counts int8[],
	children_commands_completes boolean[],
	children_commands_counts int8[],
	children_commands_depths int8[],
	children_commands_weights int8[],
	children_outputs_completes boolean[],
	children_outputs_counts int8[],
	children_outputs_depths int8[],
	children_outputs_weights int8[],
	command_completes boolean[],
	command_counts int8[],
	command_depths int8[],
	command_weights int8[],
	output_completes boolean[],
	output_counts int8[],
	output_depths int8[],
	output_weights int8[],
	children bytea[],
	child_process_indices int8[],
	child_positions int8[],
	objects bytea[],
	object_kinds int8[],
	object_process_indices int8[]
)
language plpgsql
as $$
declare
	inserted_ids bytea[];
	dummy_count int8;
begin
	with locked as (
		select processes.id
		from processes
		where processes.id = any(process_ids)
		order by processes.id
		for update
	)
	select count(*) into dummy_count from locked;

	with upsert as (
		insert into processes (id, children_complete, children_count, children_commands_complete, children_commands_count, children_commands_depth, children_commands_weight, children_outputs_complete, children_outputs_count, children_outputs_depth, children_outputs_weight, command_complete, command_count, command_depth, command_weight, output_complete, output_count, output_depth, output_weight, touched_at, transaction_id)
		select id, children_complete, children_count, children_commands_complete, children_commands_count, children_commands_depth, children_commands_weight, children_outputs_complete, children_outputs_count, children_outputs_depth, children_outputs_weight, command_complete, command_count, command_depth, command_weight, output_complete, output_count, output_depth, output_weight, touched_at, (select id from transaction_id)
		from unnest(process_ids, children_completes, children_counts, children_commands_completes, children_commands_counts, children_commands_depths, children_commands_weights, children_outputs_completes, children_outputs_counts, children_outputs_depths, children_outputs_weights, command_completes, command_counts, command_depths, command_weights, output_completes, output_counts, output_depths, output_weights, touched_ats) as t (id, children_complete, children_count, children_commands_complete, children_commands_count, children_commands_depth, children_commands_weight, children_outputs_complete, children_outputs_count, children_outputs_depth, children_outputs_weight, command_complete, command_count, command_depth, command_weight, output_complete, output_count, output_depth, output_weight, touched_at)
		on conflict (id) do update set
			children_complete = processes.children_complete or excluded.children_complete,
			children_count = coalesce(processes.children_count, excluded.children_count),
			children_commands_complete = processes.children_commands_complete or excluded.children_commands_complete,
			children_commands_count = coalesce(processes.children_commands_count, excluded.children_commands_count),
			children_commands_depth = coalesce(processes.children_commands_depth, excluded.children_commands_depth),
			children_commands_weight = coalesce(processes.children_commands_weight, excluded.children_commands_weight),
			children_outputs_complete = processes.children_outputs_complete or excluded.children_outputs_complete,
			children_outputs_count = coalesce(processes.children_outputs_count, excluded.children_outputs_count),
			children_outputs_depth = coalesce(processes.children_outputs_depth, excluded.children_outputs_depth),
			children_outputs_weight = coalesce(processes.children_outputs_weight, excluded.children_outputs_weight),
			command_complete = processes.command_complete or excluded.command_complete,
			command_count = coalesce(processes.command_count, excluded.command_count),
			command_depth = coalesce(processes.command_depth, excluded.command_depth),
			command_weight = coalesce(processes.command_weight, excluded.command_weight),
			output_complete = processes.output_complete or excluded.output_complete,
			output_count = coalesce(processes.output_count, excluded.output_count),
			output_depth = coalesce(processes.output_depth, excluded.output_depth),
			output_weight = coalesce(processes.output_weight, excluded.output_weight),
			touched_at = excluded.touched_at
		returning id, xmax = 0 as was_inserted
	)
	select coalesce(array_agg(id), '{}') into inserted_ids
	from upsert
	where was_inserted;

	insert into process_queue (process, kind, transaction_id)
	select id, kind, (select id from transaction_id)
	from unnest(inserted_ids) as t(id)
	cross join (values (0), (1), (2), (3)) as kinds(kind);

	insert into process_children (process, position, child)
	select process_ids[process_index], position, child
	from unnest(child_process_indices, child_positions, children) as t (process_index, position, child)
	on conflict (process, child) do nothing;

	insert into process_objects (process, object, kind)
	select process_ids[process_index], object, kind
	from unnest(object_process_indices, objects, object_kinds) as t (process_index, object, kind)
	on conflict (process, object, kind) do nothing;
end;
$$;

create or replace procedure put_tags(
	tags text[],
	items bytea[]
)
language plpgsql
as $$
declare
	old_items bytea[];
	dummy_count int8;
begin
	with upserted as (
		insert into tags (tag, item)
		select tag, item
		from unnest(tags, items) as t (tag, item)
		on conflict (tag) do update
		set tag = excluded.tag, item = excluded.item
		returning OLD.item
	)
	select array_agg(item)
	into old_items
	from upserted
	where item is not null;

	if array_length(old_items, 1) > 0 then
		with locked as (
			select objects.id
			from objects
			where objects.id = any(old_items)
			order by objects.id
			for update
		)
		select count(*) into dummy_count from locked;

		update objects
		set reference_count = reference_count - item_counts.count
		from (
			select t.id, count(*) as count
			from unnest(old_items) as t (id)
			group by t.id
		) as item_counts
		where objects.id = item_counts.id;

		with locked as (
			select processes.id
			from processes
			where processes.id = any(old_items)
			order by processes.id
			for update
		)
		select count(*) into dummy_count from locked;

		update processes
		set reference_count = reference_count - item_counts.count
		from (
			select t.id, count(*) as count
			from unnest(old_items) as t (id)
			group by t.id
		) as item_counts
		where processes.id = item_counts.id;

		with locked as (
			select cache_entries.id
			from cache_entries
			where cache_entries.id = any(old_items)
			order by cache_entries.id
			for update
		)
		select count(*) into dummy_count from locked;

		update cache_entries
		set reference_count = reference_count - item_counts.count
		from (
			select t.id, count(*) as count
			from unnest(old_items) as t (id)
			group by t.id
		) as item_counts
		where cache_entries.id = item_counts.id;
	end if;

	with locked as (
		select objects.id
		from objects
		where objects.id = any(items)
		order by objects.id
		for update
	)
	select count(*) into dummy_count from locked;

	update objects
	set reference_count = reference_count + item_counts.count
	from (
		select t.id, count(*) as count
		from unnest(items) as t (id)
		group by t.id
	) as item_counts
	where objects.id = item_counts.id;

	with locked as (
		select processes.id
		from processes
		where processes.id = any(items)
		order by processes.id
		for update
	)
	select count(*) into dummy_count from locked;

	update processes
	set reference_count = reference_count + item_counts.count
	from (
		select t.id, count(*) as count
		from unnest(items) as t (id)
		group by t.id
	) as item_counts
	where processes.id = item_counts.id;

	with locked as (
		select cache_entries.id
		from cache_entries
		where cache_entries.id = any(items)
		order by cache_entries.id
		for update
	)
	select count(*) into dummy_count from locked;

	update cache_entries
	set reference_count = reference_count + item_counts.count
	from (
		select t.id, count(*) as count
		from unnest(items) as t (id)
		group by t.id
	) as item_counts
	where cache_entries.id = item_counts.id;
end;
$$;

create or replace procedure delete_tags(
	tags text[]
)
language plpgsql
as $$
declare
	deleted_items bytea[];
	dummy_count int8;
begin
	if array_length(tags, 1) > 0 then
		with deleted as (
			delete from tags t
			where t.tag = any(tags)
			returning t.item
		)
		select array_agg(item)
		into deleted_items
		from deleted;

		if array_length(deleted_items, 1) > 0 then
			with locked as (
				select processes.id
				from processes
				where processes.id = any(deleted_items)
				order by processes.id
				for update
			)
			select count(*) into dummy_count from locked;

			update processes
			set reference_count = reference_count - 1
			from (
				select distinct id
				from unnest(deleted_items) as t (id)
			) as t
			where processes.id = t.id;

			with locked as (
				select objects.id
				from objects
				where objects.id = any(deleted_items)
				order by objects.id
				for update
			)
			select count(*) into dummy_count from locked;

			update objects
			set reference_count = reference_count - 1
			from (
				select distinct id
				from unnest(deleted_items) as t (id)
			) as t
			where objects.id = t.id;
		end if;
	end if;
end;
$$;

create or replace procedure touch_objects(
	touched_ats int8[],
	object_ids bytea[]
)
language plpgsql
as $$
declare
	dummy_count int8;
begin
	if array_length(object_ids, 1) > 0 then
		with locked as (
			select objects.id
			from objects
			where objects.id = any(object_ids)
			order by objects.id
			for update
		)
		select count(*) into dummy_count from locked;

		update objects
		set touched_at = t.touched_at
		from unnest(touched_ats, object_ids) as t (touched_at, id)
		where objects.id = t.id;
	end if;
end;
$$;

create or replace procedure touch_processes(
	touched_ats int8[],
	process_ids bytea[]
)
language plpgsql
as $$
declare
	dummy_count int8;
begin
	if array_length(process_ids, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(process_ids)
			order by processes.id
			for update
		)
		select count(*) into dummy_count from locked;

		update processes
		set touched_at = t.touched_at
		from unnest(touched_ats, process_ids) as t (touched_at, id)
		where processes.id = t.id;
	end if;
end;
$$;
