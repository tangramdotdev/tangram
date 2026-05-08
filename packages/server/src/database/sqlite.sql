create table remotes (
	name text primary key,
	url text not null,
	token text
);

create table namespaces (
	id integer primary key autoincrement,
	parent integer not null default 0,
	component text not null,
	name text not null
);

create index namespaces_parent_index on namespaces (parent);

create unique index namespaces_name_index on namespaces (name);

create unique index namespaces_parent_component_index on namespaces (parent, component);

create table tags (
	namespace integer not null default 0,
	name text not null,
	item text not null,
	primary key (namespace, name)
);

create table list_cache (
	arg text not null,
	output text not null,
	timestamp integer not null
);

create unique index list_cache_arg_index on list_cache (arg);

create table users (
	id text primary key
);

create table user_emails (
	"user" text not null,
	email text not null unique,
	primary key ("user", email)
);

create table tokens (
	id text primary key,
	"user" text not null
);
