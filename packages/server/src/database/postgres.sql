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
	"user" text,
	"group" text,
	"all" boolean not null default false,
	permission text not null,
	created_at int8 not null,
	created_by text,
	check (
		("user" is not null and "group" is null and not "all")
		or ("user" is null and "group" is not null and not "all")
		or ("user" is null and "group" is null and "all")
	),
	check (not "all" or permission = 'read')
);

create unique index namespace_grants_user_index
	on namespace_grants (namespace, "user", permission)
	where "user" is not null;

create unique index namespace_grants_group_index
	on namespace_grants (namespace, "group", permission)
	where "group" is not null;

create unique index namespace_grants_all_index
	on namespace_grants (namespace, permission)
	where "all";

create index namespace_grants_user_lookup_index
	on namespace_grants ("user", namespace, permission)
	where "user" is not null;

create index namespace_grants_group_lookup_index
	on namespace_grants ("group", namespace, permission)
	where "group" is not null;

create table namespace_visibility (
	namespace int8 not null default 0,
	"user" text,
	"group" text,
	"all" boolean not null default false,
	count int8 not null,
	check (
		("user" is not null and "group" is null and not "all")
		or ("user" is null and "group" is not null and not "all")
		or ("user" is null and "group" is null and "all")
	),
	check (count > 0)
);

create unique index namespace_visibility_user_index
	on namespace_visibility (namespace, "user")
	where "user" is not null;

create unique index namespace_visibility_group_index
	on namespace_visibility (namespace, "group")
	where "group" is not null;

create unique index namespace_visibility_all_index
	on namespace_visibility (namespace)
	where "all";

create index namespace_visibility_user_lookup_index
	on namespace_visibility ("user", namespace)
	where "user" is not null;

create index namespace_visibility_group_lookup_index
	on namespace_visibility ("group", namespace)
	where "group" is not null;

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
	"user" text,
	"group" text,
	"all" boolean not null default false,
	permission text not null,
	created_at int8 not null,
	created_by text,
	check (
		("user" is not null and "group" is null and not "all")
		or ("user" is null and "group" is not null and not "all")
		or ("user" is null and "group" is null and "all")
	),
	check (not "all" or permission = 'read')
);

create unique index tag_grants_user_index
	on tag_grants (namespace, name, "user", permission)
	where "user" is not null;

create unique index tag_grants_group_index
	on tag_grants (namespace, name, "group", permission)
	where "group" is not null;

create unique index tag_grants_all_index
	on tag_grants (namespace, name, permission)
	where "all";

create index tag_grants_user_lookup_index
	on tag_grants ("user", namespace, name, permission)
	where "user" is not null;

create index tag_grants_group_lookup_index
	on tag_grants ("group", namespace, name, permission)
	where "group" is not null;

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
