create table processes (
	actual_checksum text,
	cacheable boolean not null,
	command text not null,
	created_at int8 not null,
	dequeued_at int8,
	depth int8,
	enqueued_at int8,
	error text,
	error_code text,
	exit int8,
	expected_checksum text,
	finished_at int8,
	heartbeat_at int8,
	host text not null,
	id text primary key,
	log text,
	mounts text,
	network boolean not null,
	output text,
	retry boolean not null,
	started_at int8,
	status text not null,
	stderr text,
	stdin text,
	stdout text,
	token_count int8 not null,
	touched_at int8
);

create index processes_command_index on processes (command);

create index processes_depth_index on processes (depth) where status = 'started';

create index processes_heartbeat_at_index on processes (heartbeat_at) where status = 'started';

create index processes_status_index on processes (status);

create index processes_token_count_index on processes (token_count) where token_count = 0 and status != 'finished';

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

create table process_tokens (
	process text not null,
	token text not null
);

create index process_tokens_process_index on process_tokens (process);

create index process_tokens_token_index on process_tokens (token);

create table process_children (
	process text not null,
	child text not null,
	position int8 not null,
	options text,
	token text
);

create unique index process_children_process_child_index on process_children (process, child);

create index process_children_index on process_children (process, position);

create index process_children_child_process_index on process_children (child, process);

create table remotes (
	name text primary key,
	url text not null
);

create table tags (
	id bigserial primary key,
	component text not null,
	item text
);

create table tag_children (
	tag int8 not null default 0,
	child int8 not null unique,
	primary key (tag, child)
);

create table remote_tag_list_cache (
	arg text not null,
	output text not null,
	timestamp int8 not null
);

create unique index remote_tag_list_cache_arg_index on remote_tag_list_cache (arg);

create table users (
	id text primary key,
	email text not null
);

create table tokens (
	id text primary key,
	"user" text not null
);
