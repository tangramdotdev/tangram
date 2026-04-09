create table processes (
	actual_checksum text,
	cacheable integer not null,
	command text not null,
	created_at integer not null,
	depth integer,
	error text,
	error_code text,
	exit integer,
	expected_checksum text,
	finished_at integer,
	host text not null,
	id text primary key,
	log text,
	output text,
	retry integer not null,
	sandbox text not null,
	started_at integer,
	status text not null,
	stderr text,
	stdin text,
	stdout text,
	token_count integer not null,
	touched_at integer,
	tty text
);

create index processes_command_index on processes (command);

create index processes_depth_index on processes (depth) where status = 'started';

create index processes_sandbox_index on processes (sandbox);

create index processes_queue_index on processes (sandbox, created_at, id) where status = 'created';

create index processes_status_index on processes (status);

create index processes_token_count_index on processes (token_count) where token_count = 0 and status != 'finished';

create table sandboxes (
	created_at integer not null,
	finished_at integer,
	heartbeat_at integer,
	hostname text,
	id text primary key,
	mounts text,
	network integer not null,
	started_at integer,
	status text not null,
	ttl integer not null,
	"user" text
);

create index sandboxes_heartbeat_at_index on sandboxes (heartbeat_at) where status = 'started';

create index sandboxes_status_index on sandboxes (status);

create index sandboxes_queue_index on sandboxes (created_at, id) where status = 'created';

create table process_tokens (
	process text not null,
	token text not null
);

create unique index process_tokens_process_token_index on process_tokens (process, token);

create index process_tokens_process_index on process_tokens (process);

create index process_tokens_token_index on process_tokens (token);

create table process_children (
	process text not null,
	cached integer not null,
	child text not null,
	options text,
	position integer not null,
	token text
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_process_position_index on process_children (process, position);

create index process_children_child_index on process_children (child);
