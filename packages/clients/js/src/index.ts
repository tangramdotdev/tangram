import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert, todo, unimplemented, unreachable } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import {
	type ArchiveFormat,
	archive,
	bundle,
	type CompressionFormat,
	compress,
	decompress,
	download,
	extract,
} from "./builtin.ts";
import { Checksum, checksum } from "./checksum.ts";
import { client } from "./client.ts";
import { Command, command } from "./command.ts";
import { Diagnostic } from "./diagnostic.ts";
import { Directory, directory } from "./directory.ts";
import { type Encoding, encoding, setEncoding } from "./encoding.ts";
import { Error, error } from "./error.ts";
import { File, file } from "./file.ts";
import type { Grant } from "./grant.ts";
import { Graph, graph } from "./graph.ts";
import { type Host, host, setHost } from "./host.ts";
import { Request, Response, Uri } from "./http.ts";
import { Location } from "./location.ts";
import { Module } from "./module.ts";
import { Mutation, mutation } from "./mutation.ts";
import { Object } from "./object.ts";
import { path } from "./path.ts";
import { output, Placeholder, placeholder } from "./placeholder.ts";
import { Process, process, setProcess } from "./process.ts";
import { Progress } from "./progress.ts";
import type { Range } from "./range.ts";
import { Reference } from "./reference.ts";
import { Referent } from "./referent.ts";
import type { Resolved, Unresolved } from "./resolve.ts";
import { resolve } from "./resolve.ts";
import { Sandbox } from "./sandbox.ts";
import { sleep } from "./sleep.ts";
import { Symlink, symlink } from "./symlink.ts";
import type { Tag } from "./tag.ts";
import { Template, template } from "./template.ts";
import type {
	Function,
	MaybeMutation,
	MaybeMutationMap,
	MaybePromise,
	MaybeReferent,
	MutationMap,
	ResolvedArgs,
	ResolvedReturnValue,
	ReturnValue,
	UnresolvedArgs,
	ValueOrMaybeMutationMap,
} from "./util.ts";
import { Value } from "./value.ts";

export type {
	ArchiveFormat,
	CompressionFormat,
	Encoding,
	Function,
	Grant,
	Host,
	MaybeMutation,
	MaybeMutationMap,
	MaybePromise,
	MaybeReferent,
	MutationMap,
	Range,
	Resolved,
	ResolvedArgs,
	ResolvedReturnValue,
	ReturnValue,
	Tag,
	Unresolved,
	UnresolvedArgs,
	ValueOrMaybeMutationMap,
};

export type { Checkin } from "./client/checkin.ts";
export type { Checkout } from "./client/checkout.ts";
export type { Read } from "./client/read.ts";
export type { Signal } from "./client/process/signal.ts";
export type { Write } from "./client/write.ts";

let build = Process.build;
let exec = Process.exec;
let run = Process.run;
let spawn = Process.spawn;

export {
	Args,
	Artifact,
	Blob,
	Checksum,
	Command,
	Diagnostic,
	Directory,
	Error,
	File,
	Graph,
	Location,
	Module,
	Mutation,
	Object,
	Placeholder,
	Process,
	Progress,
	Reference,
	Referent,
	Request,
	Response,
	Sandbox,
	Symlink,
	Template,
	Uri,
	Value,
	archive,
	assert,
	blob,
	build,
	bundle,
	checksum,
	client,
	command,
	compress,
	decompress,
	directory,
	download,
	encoding,
	error,
	exec,
	extract,
	file,
	graph,
	host,
	mutation,
	output,
	path,
	placeholder,
	process,
	resolve,
	run,
	setEncoding,
	setHost,
	setProcess,
	sleep,
	spawn,
	symlink,
	template,
	todo,
	unimplemented,
	unreachable,
};
