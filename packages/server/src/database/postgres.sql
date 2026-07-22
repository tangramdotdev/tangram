create table nodes (
	id text primary key,
	kind text not null check (kind in ('user', 'group', 'organization', 'tag')),
	parent text,
	name text not null,
	specifier text not null unique,
	foreign key (parent) references nodes (id)
);

create unique index nodes_parent_name_index on nodes (coalesce(parent, ''), name);

create index nodes_parent_index on nodes (parent);

create index nodes_kind_index on nodes (kind);

create table users (
	id text primary key,
	name text not null,
	foreign key (id) references nodes (id)
);

create table user_emails (
	"user" text not null,
	email text not null unique,
	primary key ("user", email),
	foreign key ("user") references users (id)
);

create table logins (
	code text primary key,
	provider text not null check (provider in ('insecure', 'github')),
	status text not null check (status in ('started', 'finished')),
	"user" text,
	token text,
	error text,
	name text,
	email text,
	state text unique,
	claimed_at int8,
	expires_at int8 not null,
	interval int8 not null,
	created_at int8 not null,
	updated_at int8 not null,
	foreign key ("user") references users (id)
);

create table user_tokens (
	token text primary key,
	"user" text not null,
	foreign key ("user") references users (id)
);

create table user_identities (
	provider text not null,
	subject text not null,
	"user" text not null,
	primary key (provider, subject),
	foreign key ("user") references users (id)
);

create table github_identities (
	"user" text primary key,
	github_user_id text not null unique,
	login text not null,
	name text,
	email text,
	avatar_url text,
	html_url text,
	access_token text not null,
	refresh_token text,
	token_type text,
	scope text,
	expires_at int8,
	refresh_token_expires_at int8,
	updated_at int8 not null,
	foreign key ("user") references users (id)
);

create table runner_tokens (
	token text primary key,
	runner text not null
);

create table schedulers (
	id text primary key,
	status text not null
);

create table runners (
	id text primary key,
	scheduler text,
	status text not null
);

create index runners_scheduler_index on runners (scheduler);

create table groups (
	id text primary key,
	name text not null,
	parent text,
	foreign key (id) references nodes (id),
	foreign key (parent) references nodes (id)
);

create index groups_parent_index on groups (parent);

create table organizations (
	id text primary key,
	name text not null,
	foreign key (id) references nodes (id)
);

create table group_members (
	"group" text not null,
	member text not null,
	primary key ("group", member),
	foreign key ("group") references groups (id),
	foreign key (member) references nodes (id)
);

create index group_members_member_index on group_members (member);

create table organization_members (
	organization text not null,
	member text not null,
	primary key (organization, member),
	foreign key (organization) references organizations (id),
	foreign key (member) references nodes (id)
);

create index organization_members_member_index on organization_members (member);

create table grants (
	resource text not null,
	principal text not null,
	permissions text not null,
	created_at int8 not null,
	creator text not null,
	unique (resource, principal, creator)
);

create index grants_resource_index on grants (resource);

create index grants_principal_index on grants (principal);

create table tags (
	id text primary key,
	name text not null,
	parent text,
	item text not null,
	permissions text not null,
	foreign key (id) references nodes (id),
	foreign key (parent) references nodes (id)
);

create table list_cache (
	arg text not null,
	output text not null,
	timestamp int8 not null
);

create unique index list_cache_arg_index on list_cache (arg);

create table remotes (
	principal text,
	name text not null,
	url text not null,
	token text
);

create unique index remotes_principal_name_index on remotes (coalesce(principal, ''), name);

create table outbox (
	region text not null,
	partition int8 not null,
	id bytea not null,
	payload bytea not null,
	primary key (region, partition, id)
);
