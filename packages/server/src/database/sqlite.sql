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

create table process_tokens (
	process text not null,
	token text not null
);

create index process_tokens_process_index on process_tokens (process);

create index process_tokens_token_index on process_tokens (token);

create table process_children (
	process text not null,
	child text not null,
	options text,
	position integer not null,
	token text
);

create unique index process_children_process_child_index on process_children (process, child);

create unique index process_children_process_position_index on process_children (process, position);

create index process_children_child_index on process_children (child);

create table remotes (
	name text primary key,
	url text not null
);

create table tags (
	id integer primary key autoincrement,
	parent integer not null default 0,
	component text not null,
	item text
);

create unique index tags_parent_component_index on tags (parent, component);
create unique index tags_parent_id_index on tags (parent, id);

create table users (
	id text primary key,
	email text not null
);

create table tokens (
	id text primary key,
	"user" text not null
);
