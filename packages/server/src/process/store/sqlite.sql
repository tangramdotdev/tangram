create table sandboxes (
	cpu integer,
	created_at integer not null,
	created_by text,
	finished_at integer,
	heartbeat_at integer,
	hostname text,
	id text primary key,
	isolation text,
	memory integer,
	mounts text,
	network text,
	started_at integer,
	status text not null,
	ttl integer,
	"user" text
);

create index sandboxes_heartbeat_at_index on sandboxes (heartbeat_at) where status = 'started';

create index sandboxes_status_index on sandboxes (status);

create index sandboxes_queue_index on sandboxes (created_at, id) where status = 'created';

create table sandbox_tokens (
	sandbox text not null,
	token text primary key
);

create index sandbox_tokens_sandbox_index on sandbox_tokens (sandbox);

create table processes (
	actual_checksum text,
	cacheable integer not null,
	command text not null,
	created_at integer not null,
	created_by text,
	debug text,
	depth integer,
	error text,
	error_code text,
	exit integer,
	expected_checksum text,
	finished_at integer,
	host text not null,
	id text primary key,
	lease_count integer not null,
	log text,
	output text,
	retry integer not null,
	sandbox text not null,
	started_at integer,
	status text not null,
	stderr text,
	stderr_open integer,
	stdin text,
	stdin_open integer,
	stdout text,
	stdout_open integer,
	stored_at integer not null,
	tty text
);

create index processes_command_index on processes (command);

create index processes_depth_index on processes (depth) where status = 'started';

create index processes_lease_count_index on processes (lease_count) where lease_count = 0 and status != 'finished';

create index processes_sandbox_index on processes (sandbox);

create index processes_queue_index on processes (sandbox, created_at, id) where status = 'created';

create index processes_status_index on processes (status);

create table process_tokens (
	process text not null,
	token text primary key
);

create index process_tokens_process_index on process_tokens (process);

create table process_leases (
	process text not null,
	lease text not null
);

create unique index process_leases_process_lease_index on process_leases (process, lease);

create index process_leases_process_index on process_leases (process);

create index process_leases_lease_index on process_leases (lease);

create table process_children (
	process text not null,
	cached integer not null,
	child text not null,
	options text,
	position integer not null,
	lease text
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_process_position_index on process_children (process, position);

create index process_children_child_index on process_children (child);

create table process_signals (
	position integer primary key autoincrement,
	process text not null,
	signal text not null
);

create index process_signals_process_position_index on process_signals (process, position);

create table process_stdio (
	process text not null,
	stream text not null,
	position integer primary key autoincrement,
	bytes blob not null
);

create index process_stdio_process_stream_position_index on process_stdio (process, stream, position);

create table process_finalize_queue (
	position integer primary key autoincrement,
	created_at integer not null,
	finished_at integer,
	process text not null unique,
	started_at integer,
	status text not null
);

create index process_finalize_queue_status_position_index on process_finalize_queue (status, position);

create table sandbox_finalize_queue (
	position integer primary key autoincrement,
	created_at integer not null,
	finished_at integer,
	sandbox text not null unique,
	started_at integer,
	status text not null
);

create index sandbox_finalize_queue_status_position_index on sandbox_finalize_queue (status, position);
