create table processes (
	actual_checksum text,
	cacheable integer not null,
	command text not null,
	created_at integer not null,
	dequeued_at integer,
	depth integer,
	enqueued_at integer,
	error text,
	error_code text,
	exit integer,
	expected_checksum text,
	finished_at integer,
	heartbeat_at integer,
	host text not null,
	id text primary key,
	log text,
	mounts text,
	network integer not null,
	output text,
	retry integer not null,
	started_at integer,
	status text not null,
	stderr text,
	stdin text,
	stdout text,
	token_count integer not null,
	touched_at integer
);

create index processes_command_index on processes (command);

create index processes_depth_index on processes (depth) where status = 'started';

create index processes_heartbeat_at_index on processes (heartbeat_at) where status = 'started';

create index processes_status_index on processes (status);

create index processes_token_count_index on processes (token_count) where token_count = 0 and status != 'finished';

create trigger processes_delete_trigger
after delete on processes
for each row
begin
	delete from process_children
	where process = old.id;

	delete from process_tokens
	where process = old.id;
end;

create trigger processes_update_depth_trigger
after update of depth on processes
for each row
when new.depth <> old.depth
begin
	update processes
	set depth = max(processes.depth, new.depth + 1)
	where processes.id in (
		select process
		from process_children
		where process_children.child = new.id
	);
end;

create table process_tokens (
	process text not null,
	token text not null
);

create index process_tokens_process_index on process_tokens (process);

create index process_tokens_token_index on process_tokens (token);

create trigger process_tokens_insert_trigger
after insert on process_tokens
for each row
begin
	update processes
	set token_count = token_count + 1
	where id = new.process;
end;

create trigger process_tokens_delete_trigger
after delete on process_tokens
for each row
begin
	update processes
	set token_count = token_count - 1
	where id = old.process;
end;

create table process_children (
	process text not null,
	child text not null,
	position integer not null,
	path text,
	tag text,
	token text
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_process_position_index on process_children (process, position);

create index process_children_child_index on process_children (child);

create trigger process_children_insert_trigger
after insert on process_children
for each row
begin
	update processes
	set depth = max(
		processes.depth,
		(select depth from processes where id = new.child)
	)
	where processes.id = new.process;
end;

create table pipes (
	id text primary key,
	created_at integer not null
);

create table ptys (
	id text primary key,
	created_at integer not null,
	size text
);

create table remotes (
	name text primary key,
	url text not null
);

create table tags (
	tag text primary key,
	item text not null
);

create table users (
	id text primary key,
	email text not null
);

create table tokens (
	id text primary key,
	"user" text not null
);
