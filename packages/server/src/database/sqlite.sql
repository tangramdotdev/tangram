create table remotes (
	name text primary key,
	url text not null,
	token text
);

create table tags (
	id integer primary key autoincrement,
	component text not null,
	item text
);

create table tag_children (
	tag integer not null default 0,
	child integer not null unique,
	primary key (tag, child)
);

create table tag_list_cache (
	arg text not null,
	output text not null,
	timestamp integer not null
);

create unique index tag_list_cache_arg_index on tag_list_cache (arg);

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
