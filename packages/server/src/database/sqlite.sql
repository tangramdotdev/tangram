create table specifiers (
	id text primary key,
	specifier text not null
);

create unique index specifiers_specifier_index on specifiers (specifier);

create table users (
	id text primary key,
	name text not null,
	stripe_customer_id text unique,
	stripe_default_payment_method_id text,
	foreign key (id) references specifiers (id)
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
	claimed_at integer,
	expires_at integer not null,
	interval integer not null,
	created_at integer not null,
	updated_at integer not null,
	foreign key ("user") references users (id)
);

create table user_tokens (
	id text primary key,
	token text not null unique,
	"user" text not null,
	created_at integer not null,
	foreign key ("user") references users (id)
);

create index user_tokens_user_index on user_tokens ("user");

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
	expires_at integer,
	refresh_token_expires_at integer,
	updated_at integer not null,
	foreign key ("user") references users (id)
);

create table groups (
	id text primary key,
	name text not null,
	parent text,
	foreign key (id) references specifiers (id),
	foreign key (parent) references specifiers (id)
);

create index groups_parent_index on groups (parent);

create table organizations (
	id text primary key,
	name text not null,
	stripe_customer_id text unique,
	stripe_default_payment_method_id text,
	foreign key (id) references specifiers (id)
);

create table runners (
	id text primary key,
	owner text,
	created_at integer not null,
	foreign key (owner) references specifiers (id)
);

create index runners_owner_index on runners (owner);

create table runner_tokens (
	id text primary key,
	token text not null unique,
	runner text not null,
	created_at integer not null,
	foreign key (runner) references runners (id)
);

create index runner_tokens_runner_index on runner_tokens (runner);

create table stripe_webhooks (
	id text primary key,
	created_at integer not null
);

create index stripe_webhooks_created_at_index on stripe_webhooks (created_at);

create table group_members (
	"group" text not null,
	member text not null,
	primary key ("group", member),
	foreign key ("group") references groups (id),
	foreign key (member) references specifiers (id)
);

create index group_members_member_index on group_members (member);

create table organization_members (
	organization text not null,
	member text not null,
	primary key (organization, member),
	foreign key (organization) references organizations (id),
	foreign key (member) references specifiers (id)
);

create index organization_members_member_index on organization_members (member);

create table grants (
	resource text not null,
	subject text not null,
	permissions text not null,
	created_at integer not null,
	creator text not null,
	unique (resource, subject, creator)
);

create index grants_resource_index on grants (resource);

create index grants_subject_index on grants (subject);

create table tags (
	id text primary key,
	name text not null,
	parent text,
	target text not null,
	permissions text not null,
	foreign key (id) references specifiers (id),
	foreign key (parent) references specifiers (id)
);

create index tags_parent_index on tags (parent);

create table remote_cache (
	principal text not null,
	remote text not null,
	request text not null,
	response text not null,
	timestamp integer not null,
	primary key (principal, remote, request)
);

create table remotes (
	principal text,
	name text not null,
	url text not null,
	token text
);

create unique index remotes_principal_name_index on remotes (coalesce(principal, ''), name);

create table outbox_batch (
	next integer not null
);

insert into outbox_batch (next) values (0);

create table outbox (
	region text not null,
	batch integer not null,
	payload blob not null,
	primary key (region, batch)
);
