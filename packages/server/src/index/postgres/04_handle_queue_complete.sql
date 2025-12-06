create or replace procedure handle_queue_complete_object(
	inout n int8
)
language plpgsql
as $$
declare
	dequeued_objects bytea[];
	complete_objects bytea[];
	dummy_count int8;
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
	select count(*) into dummy_count from locked;

	with already_complete as (
		select objects.id
		from objects
		where objects.id = any(dequeued_objects)
		and objects.complete = true
	),
	updated_to_complete as (
		update objects
		set
			complete = updates.complete,
			count = coalesce(objects.count, updates.count),
			depth = coalesce(objects.depth, updates.depth),
			weight = coalesce(objects.weight, updates.weight)
		from (
			select
				objects.id,
				case
					when count(object_children.child) = 0
						then true
					when bool_and(coalesce(child_objects.complete, false))
						then true
					else false
		    end as complete,
				1 + coalesce(sum(coalesce(child_objects.count, 0)), 0) as count,
				1 + coalesce(max(coalesce(child_objects.depth, 0)), 0) as depth,
				objects.size + coalesce(sum(coalesce(child_objects.weight, 0)), 0) as weight
			from objects
			left join object_children on object_children.object = objects.id
			left join objects as child_objects on child_objects.id = object_children.child
			where objects.id = any(dequeued_objects)
			and objects.complete = false
			group by objects.id, objects.size
		) as updates
		where objects.id = updates.id
		and updates.complete = true
		returning objects.id
	)
	select array_agg(distinct id) into complete_objects
	from (
		select id from already_complete
		union all
		select id from updated_to_complete
	) all_complete;

	if complete_objects is not null and array_length(complete_objects, 1) > 0 then
		insert into object_queue (object, kind, transaction_id)
		select distinct object_children.object, 1, (select id from transaction_id)
		from object_children
		join objects on objects.id = object_children.object
		where object_children.child = any(complete_objects)
		and objects.complete = false;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_objects.process, 2, (select id from transaction_id)
		from process_objects
		join processes on processes.id = process_objects.process
		where process_objects.object = any(complete_objects)
		and processes.children_commands_complete = false;

		insert into process_queue (process, kind, transaction_id)
		select distinct process_objects.process, 3, (select id from transaction_id)
		from process_objects
		join processes on processes.id = process_objects.process
		where process_objects.object = any(complete_objects)
		and processes.children_outputs_complete = false;
	end if;

	n := n - coalesce(array_length(dequeued_objects, 1), 0);
end;
$$;

create or replace procedure handle_queue_complete_process(
	inout n int8
)
language plpgsql
as $$
declare
	current_transaction_id int8;
	children_processes bytea[];
	commands_processes bytea[];
	outputs_processes bytea[];
	children_complete_processes bytea[];
	children_commands_complete_processes bytea[];
	children_outputs_complete_processes bytea[];
	dummy_count int8;
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
		select count(*) into dummy_count from locked;

		with already_complete as (
			select id
			from processes
			where id = any(children_processes)
			and children_complete = true
		),
		updated_to_complete as (
			update processes
			set
				children_complete = updates.children_complete,
				children_count = updates.children_count
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
							then true
						when bool_and(coalesce(child_processes.children_complete, false))
							then true
						else false
					end as children_complete,
					1 + sum(child_processes.children_count) as children_count
				from processes
				left join process_children on process_children.process = processes.id
				left join processes as child_processes on child_processes.id = process_children.child
				where processes.id = any(children_processes)
				and processes.children_complete = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.children_complete = true
			returning processes.id
		)
		select array_agg(distinct id) into children_complete_processes
		from (
			select id from already_complete
			union all
			select id from updated_to_complete
		) all_complete;
	else
		children_complete_processes := array[]::bytea[];
	end if;

	if array_length(commands_processes, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(commands_processes)
			order by processes.id
			for update
		)
		select count(*) into dummy_count from locked;

		with already_complete as (
			select id
			from processes
			where id = any(commands_processes)
			and children_commands_complete = true
		),
		updated_to_complete as (
			update processes
			set
				children_commands_complete = updates.children_commands_complete,
				children_commands_count = updates.children_commands_count,
				children_commands_depth = updates.children_commands_depth,
				children_commands_weight = updates.children_commands_weight
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
						and bool_and(coalesce(objects.complete, false))
							then true
						when bool_and(coalesce(objects.complete, false))
						and bool_and(coalesce(child_processes.children_commands_complete, false))
							then true
						else false
					end as children_commands_complete,
					coalesce(sum(coalesce(objects.count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.children_commands_count, 0)), 0) as children_commands_count,
					greatest(
					coalesce(max(coalesce(objects.depth, 0)), 0),
					coalesce(max(coalesce(child_processes.children_commands_depth, 0)), 0)
					) as children_commands_depth,
					coalesce(sum(coalesce(objects.weight, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.children_commands_weight, 0)), 0) as children_commands_weight
				from processes
				left join process_objects on process_objects.process = processes.id and process_objects.kind = 0
				left join objects on objects.id = process_objects.object
				left join process_children on process_children.process = processes.id
				left join processes child_processes on child_processes.id = process_children.child
				where processes.id = any(commands_processes)
				and processes.children_commands_complete = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.children_commands_complete = true
			returning processes.id
		)
		select array_agg(distinct id) into children_commands_complete_processes
		from (
			select id from already_complete
			union all
			select id from updated_to_complete
		) all_complete;

		with locked as (
			select processes.id
			from processes
			where processes.id = any(commands_processes)
			order by processes.id
			for update
		)
		select count(*) into dummy_count from locked;

		update processes
		set
			command_complete = updates.command_complete,
			command_count = updates.command_count,
			command_depth = updates.command_depth,
			command_weight = updates.command_weight
		from (
			select
				processes.id,
				bool_and(coalesce(objects.complete, false)) as command_complete,
				coalesce(sum(coalesce(objects.count, 0)), 0) as command_count,
				coalesce(max(coalesce(objects.depth, 0)), 0) as command_depth,
				coalesce(sum(coalesce(objects.weight, 0)), 0) as command_weight
			from processes
			left join process_objects on process_objects.process = processes.id and process_objects.kind = 0
			left join objects on objects.id = process_objects.object
			where processes.id = any(commands_processes)
			and processes.command_complete = false
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.command_complete = true;

	else
		children_commands_complete_processes := array[]::bytea[];
	end if;

	if array_length(outputs_processes, 1) > 0 then
		with locked as (
			select processes.id
			from processes
			where processes.id = any(outputs_processes)
			order by processes.id
			for update
		)
		select count(*) into dummy_count from locked;

		with already_complete as (
			select id
			from processes
			where id = any(outputs_processes)
			and children_outputs_complete = true
		),
		updated_to_complete as (
			update processes
			set
				children_outputs_complete = updates.children_outputs_complete,
				children_outputs_count = updates.children_outputs_count,
				children_outputs_depth = updates.children_outputs_depth,
				children_outputs_weight = updates.children_outputs_weight
			from (
				select
					processes.id,
					case
						when count(process_children.child) = 0
						and bool_and(coalesce(objects.complete, false))
							then true
						when bool_and(coalesce(objects.complete, false))
						and bool_and(coalesce(child_processes.children_outputs_complete, false))
							then true
						else false
					end as children_outputs_complete,
					coalesce(sum(coalesce(objects.count, 0)), 0)
					+ coalesce(sum(coalesce(child_processes.children_outputs_count, 0)), 0) as children_outputs_count,
					greatest(
					coalesce(max(coalesce(objects.depth, 0)), 0),
					coalesce(max(coalesce(child_processes.children_outputs_depth, 0)), 0)
					) as children_outputs_depth,
					coalesce(sum(coalesce(objects.weight, 0)), 0)
					  + coalesce(sum(coalesce(child_processes.children_outputs_weight, 0)), 0) as children_outputs_weight
				from processes
				left join process_objects on process_objects.process = processes.id and process_objects.kind = 3
				left join objects on objects.id = process_objects.object
				left join process_children on process_children.process = processes.id
				left join processes child_processes on child_processes.id = process_children.child
				where processes.id = any(outputs_processes)
				and processes.children_outputs_complete = false
				group by processes.id
			) as updates
			where processes.id = updates.id
			and updates.children_outputs_complete = true
			returning processes.id
		)
		select array_agg(distinct id) into children_outputs_complete_processes
		from (
			select id from already_complete
			union all
			select id from updated_to_complete
		) all_complete;

		with locked as (
			select processes.id
			from processes
			where processes.id = any(outputs_processes)
			order by processes.id
			for update
		)
		select count(*) into dummy_count from locked;

		update processes
		set
			output_complete = updates.output_complete,
			output_count = updates.output_count,
			output_depth = updates.output_depth,
			output_weight = updates.output_weight
		from (
			select
				processes.id,
				case
					when count(process_objects.object) = 0
						then true
					when bool_and(coalesce(objects.complete, false))
						then true
					else false
				end as output_complete,
				coalesce(sum(coalesce(objects.count, 0)), 0) as output_count,
				coalesce(max(coalesce(objects.depth, 0)), 0) as output_depth,
				coalesce(sum(coalesce(objects.weight, 0)), 0) as output_weight
			from processes
			left join process_objects on process_objects.process = processes.id and process_objects.kind = 3
			left join objects on objects.id = process_objects.object
			where processes.id = any(outputs_processes)
			and processes.output_complete = false
			group by processes.id
		) as updates
		where processes.id = updates.id
		and updates.output_complete = true;

	else
		children_outputs_complete_processes := array[]::bytea[];
	end if;

	if children_complete_processes is not null and array_length(children_complete_processes, 1) > 0 then
		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 1, (select id from transaction_id)
		from process_children
		join processes on processes.id = process_children.process
		where process_children.child = any(children_complete_processes)
		and processes.children_complete = false;
	end if;

	if children_commands_complete_processes is not null and array_length(children_commands_complete_processes, 1) > 0 then
		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 2, (select id from transaction_id)
		from process_children
		join processes on processes.id = process_children.process
		where process_children.child = any(children_commands_complete_processes)
		and processes.children_commands_complete = false;
	end if;

	if children_outputs_complete_processes is not null and array_length(children_outputs_complete_processes, 1) > 0 then
		insert into process_queue (process, kind, transaction_id)
		select distinct process_children.process, 3, (select id from transaction_id)
		from process_children
		join processes on processes.id = process_children.process
		where process_children.child = any(children_outputs_complete_processes)
		and processes.children_outputs_complete = false;
	end if;

	n := n
		- coalesce(array_length(children_processes, 1), 0)
		- coalesce(array_length(commands_processes, 1), 0)
		- coalesce(array_length(outputs_processes, 1), 0);
end;
$$;
