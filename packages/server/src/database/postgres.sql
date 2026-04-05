create table remotes (
	name text primary key,
	url text not null
);

create table tags (
	id int8 primary key default unique_rowid(),
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
