create table sandboxes (
	created_at int8 not null,
	finished_at int8,
	heartbeat_at int8,
	hostname text,
	id text primary key,
	mounts text,
	network boolean not null,
	started_at int8,
	status text not null,
	ttl int8 not null,
	"user" text
);

create index sandboxes_heartbeat_at_index on sandboxes (heartbeat_at) where status = 'started';

create index sandboxes_status_index on sandboxes (status);

create index sandboxes_queue_index on sandboxes (created_at, id) where status = 'created';

create table processes (
	actual_checksum text,
	cacheable boolean not null,
	command text not null,
	created_at int8 not null,
	depth int8,
	error text,
	error_code text,
	exit int8,
	expected_checksum text,
	finished_at int8,
	host text not null,
	id text primary key,
	log text,
	output text,
	retry boolean not null,
	sandbox text not null,
	started_at int8,
	status text not null,
	stderr text,
	stderr_open boolean,
	stdin text,
	stdin_open boolean,
	stdout text,
	stdout_open boolean,
	token_count int8 not null,
	touched_at int8,
	tty text
);

create index processes_command_index on processes (command);

create index processes_depth_index on processes (depth) where status = 'started';

create index processes_sandbox_index on processes (sandbox);

create index processes_queue_index on processes (sandbox, created_at, id) where status = 'created';

create index processes_status_index on processes (status);

create index processes_token_count_index on processes (token_count) where token_count = 0 and status != 'finished';

create table process_tokens (
	process text not null,
	token text not null
);

create unique index process_tokens_process_token_index on process_tokens (process, token);

create index process_tokens_process_index on process_tokens (process);

create index process_tokens_token_index on process_tokens (token);

create table process_children (
	process text not null,
	cached boolean not null,
	child text not null,
	position int8 not null,
	options text,
	token text
);

create unique index process_children_process_child_index on process_children (process, child);

create index process_children_index on process_children (process, position);

create index process_children_child_process_index on process_children (child, process);

create table process_signals (
	position bigserial primary key,
	process text not null,
	signal text not null
);

create index process_signals_process_position_index on process_signals (process, position);

create table process_stdio (
	process text not null,
	stream text not null,
	position bigserial primary key,
	bytes bytea not null
);

create index process_stdio_process_stream_position_index on process_stdio (process, stream, position);

create table process_finalize_queue (
	position bigserial primary key,
	process text not null unique
);

create or replace procedure update_parent_depths(
	changed_process_ids text[]
)
language plpgsql
as $$
declare
	current_ids text[];
	updated_ids text[];
begin
	current_ids := changed_process_ids;

	while array_length(current_ids, 1) is not null and array_length(current_ids, 1) > 0 loop
		-- Update parents based on their children's depths
		with child_depths as (
			select process_children.process, max(processes.depth) as max_child_depth
			from process_children
			join processes on processes.id = process_children.child
			where process_children.child = any(current_ids)
			group by process_children.process
		),
		updated as (
			update processes
			set depth = greatest(processes.depth, child_depths.max_child_depth + 1)
			from child_depths
			where processes.id = child_depths.process
			and processes.depth < child_depths.max_child_depth + 1
			returning processes.id
		)
		select coalesce(array_agg(id), '{}') into updated_ids
		from updated;

		-- Exit if no parents were updated
		exit when cardinality(updated_ids) = 0;

		-- Continue with the updated parents
		current_ids := updated_ids;
	end loop;
end;
$$;
