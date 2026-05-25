create table users (
	id text primary key,
	namespace text
);

create unique index users_namespace_index on users (namespace);

create table user_emails (
	"user" text not null,
	email text not null unique,
	primary key ("user", email)
);

create table user_tokens (
	id text primary key,
	"user" text not null
);

create table runner_tokens (
	id text primary key
);

create table groups (
	id text primary key,
	namespace text not null,
	parent text
);

create unique index groups_namespace_index on groups (namespace);

create index groups_parent_index on groups (parent);

create table group_members (
	"group" text not null,
	"user" text not null,
	primary key ("group", "user")
);

create index group_members_user_index on group_members ("user");

create table namespace_grants (
	namespace int8 not null default 0,
	principal text not null,
	permission text not null,
	created_at int8 not null,
	created_by text,
	check (principal != 'all' or permission = 'read')
);

create unique index namespace_grants_index
	on namespace_grants (namespace, principal, permission);

create index namespace_grants_principal_lookup_index
	on namespace_grants (principal, namespace, permission);

create table namespace_visibility (
	namespace int8 not null default 0,
	principal text not null,
	count int8 not null,
	check (count > 0)
);

create unique index namespace_visibility_index
	on namespace_visibility (namespace, principal);

create index namespace_visibility_principal_lookup_index
	on namespace_visibility (principal, namespace);

create table namespaces (
	id int8 primary key default unique_rowid(),
	parent int8 not null default 0,
	component text not null,
	name text not null
);

create index namespaces_parent_index on namespaces (parent);

create unique index namespaces_name_index on namespaces (name);

create unique index namespaces_parent_component_index on namespaces (parent, component);

create table tags (
	namespace int8 not null default 0,
	name text not null,
	item text not null,
	primary key (namespace, name)
);

create table tag_grants (
	namespace int8 not null default 0,
	name text not null,
	principal text not null,
	permission text not null,
	created_at int8 not null,
	created_by text,
	check (principal != 'all' or permission = 'read')
);

create unique index tag_grants_index
	on tag_grants (namespace, name, principal, permission);

create index tag_grants_principal_lookup_index
	on tag_grants (principal, namespace, name, permission);

create table list_cache (
	arg text not null,
	output text not null,
	timestamp int8 not null
);

create unique index list_cache_arg_index on list_cache (arg);

create table remotes (
	"user" text,
	name text not null,
	url text not null,
	token text
);

create unique index remotes_user_name_index on remotes (coalesce("user", ''), name);
