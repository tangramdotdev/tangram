create or replace procedure handle_queue_stored_object(
	inout n int8
)
language plpgsql
as $$
declare
	dequeued_objects bytea[];
	stored_objects bytea[];
	locked_count int8;
begin
	with dequeued as (
		delete from object_queue
		where id in (
			select id
			from object_queue
			where kind = 1
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
	select count(*) into locked_count from locked;

	with already_stored as (
		select objects.id
		from objects
		where objects.id = any(dequeued_objects)
		and objects.subtree_stored = true
	),
	updated_to_stored as (
		update objects
		set
			subtree_stored = updates.subtree_stored,
			subtree_count = coalesce(objects.subtree_count, updates.subtree_count),
			subtree_depth = coalesce(objects.subtree_depth, updates.subtree_depth),
			subtree_size = coalesce(objects.subtree_size, updates.subtree_size)
		from (
			select
				objects.id,
				case
					when count(object_children.child) = 0
						then true
					when bool_and(coalesce(child_objects.subtree_stored, false))
						then true
					else false
		    end as subtree_stored,
				1 + coalesce(sum(coalesce(child_objects.subtree_count, 0)), 0) as subtree_count,
				1 + coalesce(max(coalesce(child_objects.subtree_depth, 0)), 0) as subtree_depth,
				objects.node_size + coalesce(sum(coalesce(child_objects.subtree_size, 0)), 0) as subtree_size
			from objects
			left join object_children on object_children.object = objects.id
			left join objects as child_objects on child_objects.id = object_children.child
			where objects.id = any(dequeued_objects)
			and objects.subtree_stored = false
			group by objects.id, objects.node_size
		) as updates
		where objects.id = updates.id
		and updates.subtree_stored = true
		returning objects.id
	)
	select array_agg(distinct id) into stored_objects
	from (
		select id from already_stored
		union all
		select id from updated_to_stored
	) all_stored;

	if stored_objects is not null and array_length(stored_objects, 1) > 0 then
		insert into object_queue (object, kind, transaction_id)
		select distinct object_children.object, 1, (select id from transaction_id)
		from object_children
		join objects on objects.id = object_children.object
		where object_children.child = any(stored_objects)
		and objects.subtree_stored = false;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_objects.process, 2, (select id from transaction_id)
		from process_objects
		join processes on processes.id = process_objects.process
		where process_objects.object = any(stored_objects)
		and processes.subtree_command_stored = false;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_objects.process, 3, (select id from transaction_id)
		from process_objects
		join processes on processes.id = process_objects.process
		where process_objects.object = any(stored_objects)
		and processes.subtree_output_stored = false;
	end if;

	n := n - coalesce(array_length(dequeued_objects, 1), 0);
end;
$$;

create or replace procedure handle_queue_stored_process(
	inout n int8
)
language plpgsql
as $$
declare
	current_transaction_id int8;
	children_processes bytea[];
	commands_processes bytea[];
	outputs_processes bytea[];
	subtree_stored_processes bytea[];
	subtree_command_stored_processes bytea[];
	subtree_output_stored_processes bytea[];
	locked_count int8;
begin
	with dequeued as (
		delete from process_queue
		where id in (
			select id
			from process_queue
			where kind >= 1
			order by id
			limit n
			for update skip locked
		)
		returning process, kind
	)
	select
		coalesce(array_agg(process) filter (where kind = 1), '{}') as children_processes,
		coalesce(array_agg(process) filter (where kind = 2), '{}') as commands_processes,
		coalesce(array_agg(process) filter (where kind = 3), '{}') as outputs_processes
	into children_processes, commands_processes, outputs_processes
	from dequeued;

	if array_length(children_processes, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(children_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		with already_stored as (
			select id
			from processes
			where id = any(children_processes)
			and subtree_stored = true
		),
		updated_to_stored as (
			update processes
			set
				subtree_stored = updates.subtree_stored,
				subtree_count = updates.subtree_count
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
							then true
						when bool_and(coalesce(child_processes.subtree_stored, false))
							then true
						else false
					end as subtree_stored,
					1 + sum(child_processes.subtree_count) as subtree_count
				from processes
				left join process_children on process_children.process = processes.id
				left join processes as child_processes on child_processes.id = process_children.child
				where processes.id = any(children_processes)
				and processes.subtree_stored = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.subtree_stored = true
			returning processes.id
		)
		select array_agg(distinct id) into subtree_stored_processes
		from (
			select id from already_stored
			union all
			select id from updated_to_stored
		) all_stored;
	else
		subtree_stored_processes := array[]::bytea[];
	end if;

	if array_length(commands_processes, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(commands_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		with already_stored as (
			select id
			from processes
			where id = any(commands_processes)
			and subtree_command_stored = true
		),
		updated_to_stored as (
			update processes
			set
				subtree_command_stored = updates.subtree_command_stored,
				subtree_command_count = updates.subtree_command_count,
				subtree_command_depth = updates.subtree_command_depth,
				subtree_command_size = updates.subtree_command_size
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
						and bool_and(coalesce(objects.subtree_stored, false))
							then true
						when bool_and(coalesce(objects.subtree_stored, false))
						and bool_and(coalesce(child_processes.subtree_command_stored, false))
							then true
						else false
					end as subtree_command_stored,
					coalesce(sum(coalesce(objects.subtree_count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_command_count, 0)), 0) as subtree_command_count,
					greatest(
					coalesce(max(coalesce(objects.subtree_depth, 0)), 0),
					coalesce(max(coalesce(child_processes.subtree_command_depth, 0)), 0)
					) as subtree_command_depth,
					coalesce(sum(coalesce(objects.subtree_size, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_command_size, 0)), 0) as subtree_command_size
				from processes
				left join process_objects on process_objects.process = processes.id and process_objects.kind = 0
				left join objects on objects.id = process_objects.object
				left join process_children on process_children.process = processes.id
				left join processes child_processes on child_processes.id = process_children.child
				where processes.id = any(commands_processes)
				and processes.subtree_command_stored = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.subtree_command_stored = true
			returning processes.id
		)
		select array_agg(distinct id) into subtree_command_stored_processes
		from (
			select id from already_stored
			union all
			select id from updated_to_stored
		) all_stored;

		with locked as (
			select processes.id
			from processes
			where processes.id = any(commands_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		update processes
		set
			node_command_stored = updates.node_command_stored,
			node_command_count = updates.node_command_count,
			node_command_depth = updates.node_command_depth,
			node_command_size = updates.node_command_size
		from (
			select
				processes.id,
				bool_and(coalesce(objects.subtree_stored, false)) as node_command_stored,
				coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_command_count,
				coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_command_depth,
				coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_command_size
			from processes
			left join process_objects on process_objects.process = processes.id and process_objects.kind = 0
			left join objects on objects.id = process_objects.object
			where processes.id = any(commands_processes)
			and processes.node_command_stored = false
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.node_command_stored = true;

	else
		subtree_command_stored_processes := array[]::bytea[];
	end if;

	if array_length(outputs_processes, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(outputs_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		with already_stored as (
			select id
			from processes
			where id = any(outputs_processes)
			and subtree_output_stored = true
		),
		updated_to_stored as (
			update processes
			set
				subtree_output_stored = updates.subtree_output_stored,
				subtree_output_count = updates.subtree_output_count,
				subtree_output_depth = updates.subtree_output_depth,
				subtree_output_size = updates.subtree_output_size
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
						and bool_and(coalesce(objects.subtree_stored, false))
							then true
						when bool_and(coalesce(objects.subtree_stored, false))
						and bool_and(coalesce(child_processes.subtree_output_stored, false))
							then true
						else false
					end as subtree_output_stored,
					coalesce(sum(coalesce(objects.subtree_count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_output_count, 0)), 0) as subtree_output_count,
					greatest(
					coalesce(max(coalesce(objects.subtree_depth, 0)), 0),
					coalesce(max(coalesce(child_processes.subtree_output_depth, 0)), 0)
					) as subtree_output_depth,
					coalesce(sum(coalesce(objects.subtree_size, 0)), 0)
					  + coalesce(sum(coalesce(child_processes.subtree_output_size, 0)), 0) as subtree_output_size
				from processes
				left join process_objects on process_objects.process = processes.id and process_objects.kind = 3
				left join objects on objects.id = process_objects.object
				left join process_children on process_children.process = processes.id
				left join processes child_processes on child_processes.id = process_children.child
				where processes.id = any(outputs_processes)
				and processes.subtree_output_stored = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.subtree_output_stored = true
			returning processes.id
		)
		select array_agg(distinct id) into subtree_output_stored_processes
		from (
			select id from already_stored
			union all
			select id from updated_to_stored
		) all_stored;

		with locked as (
			select processes.id
			from processes
			where processes.id = any(outputs_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		update processes
		set
			node_output_stored = updates.node_output_stored,
			node_output_count = updates.node_output_count,
			node_output_depth = updates.node_output_depth,
			node_output_size = updates.node_output_size
		from (
			select
				processes.id,
				case
					when count(process_objects.object) = 0
						then true
					when bool_and(coalesce(objects.subtree_stored, false))
						then true
					else false
				end as node_output_stored,
				coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_output_count,
				coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_output_depth,
				coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_output_size
			from processes
			left join process_objects on process_objects.process = processes.id and process_objects.kind = 3
			left join objects on objects.id = process_objects.object
			where processes.id = any(outputs_processes)
			and processes.node_output_stored = false
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.node_output_stored = true;

	else
		subtree_output_stored_processes := array[]::bytea[];
	end if;

	if subtree_stored_processes is not null and array_length(subtree_stored_processes, 1) > 0 then
		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 1, (select id from transaction_id)
		from process_children
		join processes on processes.id = process_children.process
		where process_children.child = any(subtree_stored_processes)
		and processes.subtree_stored = false;
	end if;

	if subtree_command_stored_processes is not null and array_length(subtree_command_stored_processes, 1) > 0 then
		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 2, (select id from transaction_id)
		from process_children
		join processes on processes.id = process_children.process
		where process_children.child = any(subtree_command_stored_processes)
		and processes.subtree_command_stored = false;
	end if;

	if subtree_output_stored_processes is not null and array_length(subtree_output_stored_processes, 1) > 0 then
		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 3, (select id from transaction_id)
		from process_children
		join processes on processes.id = process_children.process
		where process_children.child = any(subtree_output_stored_processes)
		and processes.subtree_output_stored = false;
	end if;

	n := n
		- coalesce(array_length(children_processes, 1), 0)
		- coalesce(array_length(commands_processes, 1), 0)
		- coalesce(array_length(outputs_processes, 1), 0);
end;
$$;
