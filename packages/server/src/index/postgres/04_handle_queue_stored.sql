create or replace procedure handle_queue_stored_object(
	inout n int8
)
language plpgsql
as $$
declare
	dequeued_objects bytea[];
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

	if coalesce(array_length(dequeued_objects, 1), 0) = 0 then
		return;
	end if;

	with locked as (
		select objects.id
		from objects
		where objects.id = any(dequeued_objects)
		order by objects.id
		for update
	)
	select count(*) into locked_count from locked;

	-- Update objects where subtree is not fully computed.
	update objects
	set
		subtree_stored = updates.subtree_stored,
		subtree_count = coalesce(objects.subtree_count, updates.subtree_count),
		subtree_depth = coalesce(objects.subtree_depth, updates.subtree_depth),
		subtree_size = coalesce(objects.subtree_size, updates.subtree_size),
		subtree_solvable = coalesce(objects.subtree_solvable, updates.subtree_solvable),
		subtree_solved = coalesce(objects.subtree_solved, updates.subtree_solved)
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
			objects.node_size + coalesce(sum(coalesce(child_objects.subtree_size, 0)), 0) as subtree_size,
			objects.node_solvable or coalesce(bool_or(coalesce(child_objects.subtree_solvable, false)), false) as subtree_solvable,
			objects.node_solved and coalesce(bool_and(coalesce(child_objects.subtree_solved, true)), true) as subtree_solved
		from objects
		left join object_children on object_children.object = objects.id
		left join objects as child_objects on child_objects.id = object_children.child
		where objects.id = any(dequeued_objects)
		and (
			objects.subtree_stored = false
			or objects.subtree_count is null
			or objects.subtree_depth is null
			or objects.subtree_size is null
			or objects.subtree_solvable is null
			or objects.subtree_solved is null
		)
		group by objects.id, objects.node_size, objects.node_solvable, objects.node_solved
	) as updates
	where objects.id = updates.id
	and updates.subtree_stored = true;

	insert into object_queue (object, kind, transaction_id)
	select distinct object_children.object, 1, (select id from transaction_id)
	from object_children
	where object_children.child = any(dequeued_objects);

	insert into process_queue (process, kind, transaction_id)
	select distinct process_objects.process, 2, (select id from transaction_id)
	from process_objects
	where process_objects.object = any(dequeued_objects);

	insert into process_queue (process, kind, transaction_id)
	select distinct process_objects.process, 5, (select id from transaction_id)
	from process_objects
	where process_objects.object = any(dequeued_objects);

	insert into process_queue (process, kind, transaction_id)
	select distinct process_objects.process, 4, (select id from transaction_id)
	from process_objects
	where process_objects.object = any(dequeued_objects);

	n := n - coalesce(array_length(dequeued_objects, 1), 0);
end;
$$;

create or replace procedure handle_queue_stored_process(
	inout n int8
)
language plpgsql
as $$
declare
	children_processes bytea[];
	commands_processes bytea[];
	logs_processes bytea[];
	outputs_processes bytea[];
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
		coalesce(array_agg(process) filter (where kind = 4), '{}') as logs_processes,
		coalesce(array_agg(process) filter (where kind = 5), '{}') as outputs_processes
	into children_processes, commands_processes, logs_processes, outputs_processes
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
				1 + coalesce(sum(coalesce(child_processes.subtree_count, 0)), 0) as subtree_count
			from processes
			left join process_children on process_children.process = processes.id
			left join processes as child_processes on child_processes.id = process_children.child
			where processes.id = any(children_processes)
			and (
				processes.subtree_stored = false
				or processes.subtree_count is null
			)
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.subtree_stored = true;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 1, (select id from transaction_id)
		from process_children
		where process_children.child = any(children_processes);
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
					when
						coalesce(command_objects.subtree_stored, false)
						and (coalesce(child_processes.child_count, 0) = 0 or child_processes.all_stored)
						then true
					else false
				end as subtree_command_stored,
				coalesce(command_objects.subtree_count, 0) + coalesce(child_processes.subtree_command_count, 0) as subtree_command_count,
				greatest(coalesce(command_objects.subtree_depth, 0), coalesce(child_processes.subtree_command_depth, 0)) as subtree_command_depth,
				coalesce(command_objects.subtree_size, 0) + coalesce(child_processes.subtree_command_size, 0) as subtree_command_size
			from processes
			left join (
				select
					process_objects.process,
					objects.subtree_stored,
					objects.subtree_count,
					objects.subtree_depth,
					objects.subtree_size
				from process_objects
				left join objects on objects.id = process_objects.object
				where process_objects.kind = 0
			) as command_objects on command_objects.process = processes.id
			left join (
				select
					process_children.process,
					count(process_children.child) as child_count,
					bool_and(coalesce(child.subtree_command_stored, false)) as all_stored,
					sum(coalesce(child.subtree_command_count, 0)) as subtree_command_count,
					max(coalesce(child.subtree_command_depth, 0)) as subtree_command_depth,
					sum(coalesce(child.subtree_command_size, 0)) as subtree_command_size
				from process_children
				left join processes child on child.id = process_children.child
				group by process_children.process
			) as child_processes on child_processes.process = processes.id
			where processes.id = any(commands_processes)
			and (
				processes.subtree_command_stored = false
				or processes.subtree_command_count is null
				or processes.subtree_command_depth is null
				or processes.subtree_command_size is null
			)
		) as updates
		where processes.id = updates.id
		and updates.subtree_command_stored = true;

		update processes
		set
			node_command_stored = objects.subtree_stored,
			node_command_count = objects.subtree_count,
			node_command_depth = objects.subtree_depth,
			node_command_size = objects.subtree_size
		from process_objects
		left join objects on process_objects.object = objects.id
		where processes.id = process_objects.process
			and process_objects.kind = 0
			and process_objects.process = any(commands_processes)
			and objects.subtree_stored = true
			and (
				processes.node_command_stored = false
				or processes.node_command_count is null
				or processes.node_command_depth is null
				or processes.node_command_size is null
			);

		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 2, (select id from transaction_id)
		from process_children
		where process_children.child = any(commands_processes);
	end if;

	if array_length(logs_processes, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(logs_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		with already_stored as (
			select id
			from processes
			where id = any(logs_processes)
			and subtree_log_stored = true
		),
		updated_to_stored as (
			update processes
			set
				subtree_log_stored = updates.subtree_log_stored,
				subtree_log_count = updates.subtree_log_count,
				subtree_log_depth = updates.subtree_log_depth,
				subtree_log_size = updates.subtree_log_size
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
						and (count(process_objects.object) = 0 or bool_and(coalesce(objects.subtree_stored, false)))
							then true
						when (count(process_objects.object) = 0 or bool_and(coalesce(objects.subtree_stored, false)))
						and (count(process_children.child) = 0 or bool_and(coalesce(child_processes.subtree_log_stored, false)))
							then true
						else false
					end as subtree_log_stored,
					coalesce(sum(coalesce(objects.subtree_count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_log_count, 0)), 0) as subtree_log_count,
					greatest(
					coalesce(max(coalesce(objects.subtree_depth, 0)), 0),
					coalesce(max(coalesce(child_processes.subtree_log_depth, 0)), 0)
					) as subtree_log_depth,
					coalesce(sum(coalesce(objects.subtree_size, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.subtree_log_size, 0)), 0) as subtree_log_size
				from processes
				left join process_objects on process_objects.process = processes.id and process_objects.kind = 2
				left join objects on objects.id = process_objects.object
				left join process_children on process_children.process = processes.id
				left join processes child_processes on child_processes.id = process_children.child
				where processes.id = any(logs_processes)
				and processes.subtree_log_stored = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.subtree_log_stored = true
			returning processes.id
		)
		select array_agg(distinct id) into subtree_log_stored_processes
		from (
			select id from already_stored
			union all
			select id from updated_to_stored
		) all_stored;

		with locked as (
			select processes.id
			from processes
			where processes.id = any(logs_processes)
			order by processes.id
			for update
		)
		select count(*) into locked_count from locked;

		update processes
		set
			node_log_stored = updates.node_log_stored,
			node_log_count = updates.node_log_count,
			node_log_depth = updates.node_log_depth,
			node_log_size = updates.node_log_size
		from (
			select
				processes.id,
				case
					when count(process_objects.object) = 0 then true
					else bool_and(coalesce(objects.subtree_stored, false))
				end as node_log_stored,
				coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_log_count,
				coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_log_depth,
				coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_log_size
			from processes
			left join process_objects on process_objects.process = processes.id and process_objects.kind = 2
			left join objects on objects.id = process_objects.object
			where processes.id = any(logs_processes)
			and processes.node_log_stored = false
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.node_log_stored = true;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 4, (select id from transaction_id)
		from process_children
		where process_children.child = any(logs_processes);
	else
		subtree_log_stored_processes := array[]::bytea[];
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
					when
						(output_objects.process is null or coalesce(output_objects.subtree_stored, false))
						and (coalesce(child_processes.child_count, 0) = 0 or child_processes.all_stored)
						then true
					else false
				end as subtree_output_stored,
				coalesce(output_objects.subtree_count, 0) + coalesce(child_processes.subtree_output_count, 0) as subtree_output_count,
				greatest(coalesce(output_objects.subtree_depth, 0), coalesce(child_processes.subtree_output_depth, 0)) as subtree_output_depth,
				coalesce(output_objects.subtree_size, 0) + coalesce(child_processes.subtree_output_size, 0) as subtree_output_size
			from processes
			left join (
				select
					process_objects.process,
					objects.subtree_stored,
					objects.subtree_count,
					objects.subtree_depth,
					objects.subtree_size
				from process_objects
				left join objects on objects.id = process_objects.object
				where process_objects.kind = 3
			) as output_objects on output_objects.process = processes.id
			left join (
				select
					process_children.process,
					count(process_children.child) as child_count,
					bool_and(coalesce(child.subtree_output_stored, false)) as all_stored,
					sum(coalesce(child.subtree_output_count, 0)) as subtree_output_count,
					max(coalesce(child.subtree_output_depth, 0)) as subtree_output_depth,
					sum(coalesce(child.subtree_output_size, 0)) as subtree_output_size
				from process_children
				left join processes child on child.id = process_children.child
				group by process_children.process
			) as child_processes on child_processes.process = processes.id
			where processes.id = any(outputs_processes)
			and (
				processes.subtree_output_stored = false
				or processes.subtree_output_count is null
				or processes.subtree_output_depth is null
				or processes.subtree_output_size is null
			)
		) as updates
		where processes.id = updates.id
		and updates.subtree_output_stored = true;

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
					when count(process_objects.object) = 0 then true
					else bool_and(coalesce(objects.subtree_stored, false))
				end as node_output_stored,
				coalesce(sum(coalesce(objects.subtree_count, 0)), 0) as node_output_count,
				coalesce(max(coalesce(objects.subtree_depth, 0)), 0) as node_output_depth,
				coalesce(sum(coalesce(objects.subtree_size, 0)), 0) as node_output_size
			from processes
			left join process_objects on process_objects.process = processes.id and process_objects.kind = 3
			left join objects on objects.id = process_objects.object
			where processes.id = any(outputs_processes)
			and (
				processes.node_output_stored = false
				or processes.node_output_count is null
				or processes.node_output_depth is null
				or processes.node_output_size is null
			)
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.node_output_stored = true;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 5, (select id from transaction_id)
		from process_children
		where process_children.child = any(outputs_processes);
	end if;

	n := n
		- coalesce(array_length(children_processes, 1), 0)
		- coalesce(array_length(commands_processes, 1), 0)
		- coalesce(array_length(logs_processes, 1), 0)
		- coalesce(array_length(outputs_processes, 1), 0);
end;
$$;
