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

create table user_tokens (
	id text primary key,
	"user" text not null,
	foreign key ("user") references users (id)
);

create table runner_tokens (
	id text primary key
);

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
	permission text not null,
	created_at integer not null,
	created_by text,
	unique (resource, principal, permission)
);

create index grants_resource_index on grants (resource);

create index grants_principal_index on grants (principal);

create table visibility (
	resource text not null,
	principal text not null,
	count integer not null,
	check (count > 0),
	unique (resource, principal)
);

create index visibility_principal_resource_index on visibility (principal, resource);

create table tags (
	id text primary key,
	name text not null,
	parent text,
	item text not null,
	foreign key (id) references nodes (id),
	foreign key (parent) references nodes (id)
);

create table list_cache (
	arg text not null,
	output text not null,
	timestamp integer not null
);

create unique index list_cache_arg_index on list_cache (arg);

create table remotes (
	"user" text,
	name text not null,
	url text not null,
	token text
);

create unique index remotes_user_name_index on remotes (coalesce("user", ''), name);
