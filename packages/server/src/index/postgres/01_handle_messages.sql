create or replace procedure handle_messages(
	cache_entry_ids bytea[],
	cache_entry_touched_ats int8[],

	object_ids bytea[],
	object_cache_entries bytea[],
	object_node_sizes int8[],
	object_node_solvables bool[],
	object_node_solveds bool[],
	object_touched_ats int8[],
	object_subtree_counts int8[],
	object_subtree_depths int8[],
	object_subtree_sizes int8[],
	object_subtree_solvables bool[],
	object_subtree_solveds bool[],
	object_subtree_storeds bool[],
	object_children bytea[],
	object_parent_indices int8[],

	touch_object_touched_ats int8[],
	touch_object_ids bytea[],

	process_ids bytea[],
	process_touched_ats int8[],
	process_subtree_storeds bool[],
	process_subtree_counts int8[],
	process_subtree_command_storeds bool[],
	process_subtree_command_counts int8[],
	process_subtree_command_depths int8[],
	process_subtree_command_sizes int8[],
	process_subtree_log_storeds bool[],
	process_subtree_log_counts int8[],
	process_subtree_log_depths int8[],
	process_subtree_log_sizes int8[],
	process_subtree_output_storeds bool[],
	process_subtree_output_counts int8[],
	process_subtree_output_depths int8[],
	process_subtree_output_sizes int8[],
	process_node_command_storeds bool[],
	process_node_command_counts int8[],
	process_node_command_depths int8[],
	process_node_command_sizes int8[],
	process_node_log_storeds bool[],
	process_node_log_counts int8[],
	process_node_log_depths int8[],
	process_node_log_sizes int8[],	
	process_node_output_storeds bool[],
	process_node_output_counts int8[],
	process_node_output_depths int8[],
	process_node_output_sizes int8[],
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
		object_node_sizes,
		object_node_solvables,
		object_node_solveds,
		object_touched_ats,
		object_subtree_counts,
		object_subtree_depths,
		object_subtree_sizes,
		object_subtree_solvables,
		object_subtree_solveds,
		object_subtree_storeds,
		object_children,
		object_parent_indices
	);

	call touch_objects(touch_object_touched_ats, touch_object_ids);

	call put_processes(
		process_ids,
		process_touched_ats,
		process_subtree_storeds,
		process_subtree_counts,
		process_subtree_command_storeds,
		process_subtree_command_counts,
		process_subtree_command_depths,
		process_subtree_command_sizes,
		process_subtree_log_storeds,
		process_subtree_log_counts,
		process_subtree_log_depths,
		process_subtree_log_sizes,
		process_subtree_output_storeds,
		process_subtree_output_counts,
		process_subtree_output_depths,
		process_subtree_output_sizes,
		process_node_command_storeds,
		process_node_command_counts,
		process_node_command_depths,
		process_node_command_sizes,
		process_node_log_storeds,
		process_node_log_counts,
		process_node_log_depths,
		process_node_log_sizes,
		process_node_output_storeds,
		process_node_output_counts,
		process_node_output_depths,
		process_node_output_sizes,
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

create or replace procedure put_cache_entries(
		cache_entry_ids bytea[],
		cache_entry_touched_ats int8[]
)
language plpgsql
as $$
declare
	inserted_ids bytea[];
	locked_count int8;
begin
	if array_length(cache_entry_ids, 1) > 0 then
		with locked as (
			select cache_entries.id
			from cache_entries
			where cache_entries.id = any(cache_entry_ids)
			order by cache_entries.id
			for update
		)
		select count(*) into locked_count from locked;

		with upsert as (
			insert into cache_entries (id, touched_at)
			select id, touched_at
			from unnest(cache_entry_ids, cache_entry_touched_ats) as t (id, touched_at)
			on conflict (id) do update set
				touched_at = greatest(excluded.touched_at, cache_entries.touched_at)
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

create or replace procedure put_objects(
	ids bytea[],
	cache_entries bytea[],
	node_sizes int8[],
	node_solvables boolean[],
	node_solveds boolean[],
	touched_ats int8[],
	subtree_counts int8[],
	subtree_depths int8[],
	subtree_sizes int8[],
	subtree_solvables boolean[],
	subtree_solveds boolean[],
	subtree_storeds boolean[],
	children bytea[],
	parent_indices int8[]
)
language plpgsql
as $$
declare
	inserted_ids bytea[];
	changed_ids bytea[];
	locked_count int8;
begin
	with locked as (
		select objects.id
		from objects
		where objects.id = any(ids)
		order by objects.id
		for update
	)
	select count(*) into locked_count from locked;

	with upsert as (
		insert into objects (id, cache_entry, node_size, node_solvable, node_solved, touched_at, subtree_count, subtree_depth, subtree_size, subtree_solvable, subtree_solved, subtree_stored, transaction_id)
		select id, cache_entry, node_size, node_solvable, node_solved, touched_at, subtree_count, subtree_depth, subtree_size, subtree_solvable, subtree_solved, subtree_stored, (select id from transaction_id)
		from unnest(ids, cache_entries, node_sizes, node_solvables, node_solveds, touched_ats, subtree_counts, subtree_depths, subtree_sizes, subtree_solvables, subtree_solveds, subtree_storeds)
			as t (id, cache_entry, node_size, node_solvable, node_solved, touched_at, subtree_count, subtree_depth, subtree_size, subtree_solvable, subtree_solved, subtree_stored)
		on conflict (id) do update set
			subtree_count = coalesce(excluded.subtree_count, objects.subtree_count),
			subtree_depth = coalesce(excluded.subtree_depth, objects.subtree_depth),
			subtree_size = coalesce(excluded.subtree_size, objects.subtree_size),
			subtree_solvable = coalesce(excluded.subtree_solvable, objects.subtree_solvable),
			subtree_solved = coalesce(excluded.subtree_solved, objects.subtree_solved),
			subtree_stored = excluded.subtree_stored or objects.subtree_stored,
			touched_at = greatest(excluded.touched_at, objects.touched_at)
		returning
			objects.id,
			xmax = 0 as was_inserted,
			old.subtree_count is distinct from new.subtree_count
				or old.subtree_depth is distinct from new.subtree_depth
				or old.subtree_size is distinct from new.subtree_size
				or old.subtree_stored is distinct from new.subtree_stored
				or old.subtree_solvable is distinct from new.subtree_solvable
				or old.subtree_solved is distinct from new.subtree_solved
			as subtree_changed
	)
	select
		coalesce((select array_agg(id) from upsert where was_inserted), '{}'),
		coalesce((select array_agg(id) from upsert where was_inserted or subtree_changed), '{}')
	into inserted_ids, changed_ids;

	-- Enqueue reference count for inserted rows only.
	insert into object_queue (object, kind, transaction_id)
	select id, 0, (select id from transaction_id)
	from unnest(inserted_ids) as t(id);

	-- Enqueue stored/metadata for inserted rows or rows where subtree changed.
	insert into object_queue (object, kind, transaction_id)
	select id, 1, (select id from transaction_id)
	from unnest(changed_ids) as t(id);

	insert into object_children (object, child)
	select ids[object_index], child
	from unnest(parent_indices, children) as t (object_index, child)
	on conflict (object, child) do nothing;
end;
$$;

create or replace procedure put_processes(
	process_ids bytea[],
	touched_ats int8[],
	subtree_storeds boolean[],
	subtree_counts int8[],
	subtree_command_storeds boolean[],
	subtree_command_counts int8[],
	subtree_command_depths int8[],
	subtree_command_sizes int8[],
	subtree_log_storeds boolean[],
	subtree_log_counts int8[],
	subtree_log_depths int8[],
	subtree_log_sizes int8[],
	subtree_output_storeds boolean[],
	subtree_output_counts int8[],
	subtree_output_depths int8[],
	subtree_output_sizes int8[],
	node_command_storeds boolean[],
	node_command_counts int8[],
	node_command_depths int8[],
	node_command_sizes int8[],
	node_log_storeds boolean[],
	node_log_counts int8[],
	node_log_depths int8[],
	node_log_sizes int8[],
	node_output_storeds boolean[],
	node_output_counts int8[],
	node_output_depths int8[],
	node_output_sizes int8[],
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
	changed_ids bytea[];
	locked_count int8;
begin
	with locked as (
		select processes.id
		from processes
		where processes.id = any(process_ids)
		order by processes.id
		for update
	)
	select count(*) into locked_count from locked;

	with upsert as (
		insert into processes (id, node_command_count, node_command_depth, node_command_size, node_command_stored, node_log_count, node_log_depth, node_log_size, node_log_stored, node_output_count, node_output_depth, node_output_size, node_output_stored, subtree_command_count, subtree_command_depth, subtree_command_size, subtree_command_stored, subtree_log_count, subtree_log_depth, subtree_log_size, subtree_log_stored, subtree_output_count, subtree_output_depth, subtree_output_size, subtree_output_stored, subtree_count, subtree_stored, touched_at, transaction_id)
		select id, node_command_count, node_command_depth, node_command_size, node_command_stored, node_log_count, node_log_depth, node_log_size, node_log_stored, node_output_count, node_output_depth, node_output_size, node_output_stored, subtree_command_count, subtree_command_depth, subtree_command_size, subtree_command_stored, subtree_log_count, subtree_log_depth, subtree_log_size, subtree_log_stored, subtree_output_count, subtree_output_depth, subtree_output_size, subtree_output_stored, subtree_count, subtree_stored, touched_at, (select id from transaction_id)
		from unnest(process_ids, subtree_storeds, subtree_counts, subtree_command_storeds, subtree_command_counts, subtree_command_depths, subtree_command_sizes, subtree_log_storeds, subtree_log_counts, subtree_log_depths, subtree_log_sizes, subtree_output_storeds, subtree_output_counts, subtree_output_depths, subtree_output_sizes, node_command_storeds, node_command_counts, node_command_depths, node_command_sizes, node_log_storeds, node_log_counts, node_log_depths, node_log_sizes, node_output_storeds, node_output_counts, node_output_depths, node_output_sizes, touched_ats) as t (id, subtree_stored, subtree_count, subtree_command_stored, subtree_command_count, subtree_command_depth, subtree_command_size, subtree_log_stored, subtree_log_count, subtree_log_depth, subtree_log_size, subtree_output_stored, subtree_output_count, subtree_output_depth, subtree_output_size, node_command_stored, node_command_count, node_command_depth, node_command_size, node_log_stored, node_log_count, node_log_depth, node_log_size, node_output_stored, node_output_count, node_output_depth, node_output_size, touched_at)
		on conflict (id) do update set
			node_command_count = coalesce(processes.node_command_count, excluded.node_command_count),
			node_command_depth = coalesce(processes.node_command_depth, excluded.node_command_depth),
			node_command_size = coalesce(processes.node_command_size, excluded.node_command_size),
			node_command_stored = processes.node_command_stored or excluded.node_command_stored,
			node_log_count = coalesce(processes.node_log_count, excluded.node_log_count),
			node_log_depth = coalesce(processes.node_log_depth, excluded.node_log_depth),
			node_log_size = coalesce(processes.node_log_size, excluded.node_log_size),
			node_log_stored = processes.node_log_stored or excluded.node_log_stored,
			node_output_count = coalesce(processes.node_output_count, excluded.node_output_count),
			node_output_depth = coalesce(processes.node_output_depth, excluded.node_output_depth),
			node_output_size = coalesce(processes.node_output_size, excluded.node_output_size),
			node_output_stored = processes.node_output_stored or excluded.node_output_stored,
			subtree_command_count = coalesce(processes.subtree_command_count, excluded.subtree_command_count),
			subtree_command_depth = coalesce(processes.subtree_command_depth, excluded.subtree_command_depth),
			subtree_command_size = coalesce(processes.subtree_command_size, excluded.subtree_command_size),
			subtree_command_stored = processes.subtree_command_stored or excluded.subtree_command_stored,
			subtree_log_count = coalesce(processes.subtree_log_count, excluded.subtree_log_count),
			subtree_log_depth = coalesce(processes.subtree_log_depth, excluded.subtree_log_depth),
			subtree_log_size = coalesce(processes.subtree_log_size, excluded.subtree_log_size),
			subtree_log_stored = processes.subtree_log_stored or excluded.subtree_log_stored,
			subtree_output_count = coalesce(processes.subtree_output_count, excluded.subtree_output_count),
			subtree_output_depth = coalesce(processes.subtree_output_depth, excluded.subtree_output_depth),
			subtree_output_size = coalesce(processes.subtree_output_size, excluded.subtree_output_size),
			subtree_output_stored = processes.subtree_output_stored or excluded.subtree_output_stored,
			subtree_count = coalesce(processes.subtree_count, excluded.subtree_count),
			subtree_stored = processes.subtree_stored or excluded.subtree_stored,
			touched_at = greatest(processes.touched_at, excluded.touched_at)
		returning
			processes.id,
			xmax = 0 as was_inserted,
			old.subtree_command_count is distinct from new.subtree_command_count
				or old.subtree_command_depth is distinct from new.subtree_command_depth
				or old.subtree_command_size is distinct from new.subtree_command_size
				or old.subtree_command_stored is distinct from new.subtree_command_stored
				or old.subtree_output_count is distinct from new.subtree_output_count
				or old.subtree_output_depth is distinct from new.subtree_output_depth
				or old.subtree_output_size is distinct from new.subtree_output_size
				or old.subtree_output_stored is distinct from new.subtree_output_stored
				or old.subtree_count is distinct from new.subtree_count
				or old.subtree_stored is distinct from new.subtree_stored
			as subtree_changed
	)
	select
		coalesce((select array_agg(id) from upsert where was_inserted), '{}'),
		coalesce((select array_agg(id) from upsert where was_inserted or subtree_changed), '{}')
	into inserted_ids, changed_ids;

	-- Enqueue reference count for inserted rows only.
	insert into process_queue (process, kind, transaction_id)
	select id, 0, (select id from transaction_id)
	from unnest(inserted_ids) as t(id);

	-- Enqueue stored/metadata for inserted rows or rows where subtree changed.
	insert into process_queue (process, kind, transaction_id)
	select id, kind, (select id from transaction_id)
	from unnest(changed_ids) as t(id)
	cross join (values (1), (2), (3), (4)) as kinds(kind);

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
	locked_count int8;
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
		select count(*) into locked_count from locked;

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
		select count(*) into locked_count from locked;

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
		select count(*) into locked_count from locked;

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
	select count(*) into locked_count from locked;

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
	select count(*) into locked_count from locked;

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
	select count(*) into locked_count from locked;

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
	locked_count int8;
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
			select count(*) into locked_count from locked;

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
			select count(*) into locked_count from locked;

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
	locked_count int8;
begin
	if array_length(object_ids, 1) > 0 then
		with locked as (
			select objects.id
			from objects
			where objects.id = any(object_ids)
			order by objects.id
			for update
		)
		select count(*) into locked_count from locked;

		update objects
		set touched_at = greatest(objects.touched_at, t.touched_at)
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
	locked_count int8;
begin
	if array_length(process_ids, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(process_ids)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		update processes
		set touched_at = greatest(processes.touched_at, t.touched_at)
		from unnest(touched_ats, process_ids) as t (touched_at, id)
		where processes.id = t.id;
	end if;
end;
$$;
