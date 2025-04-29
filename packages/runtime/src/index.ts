import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert, unimplemented, unreachable } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import { BuildBuilder } from "./build.ts";
import {
	type ArchiveFormat,
	type CompressionFormat,
	archive,
	bundle,
	compress,
	decompress,
	download,
	extract,
} from "./builtin.ts";
import { Checksum, checksum } from "./checksum.ts";
import { Command, CommandBuilder, command } from "./command.ts";
import { Directory, directory } from "./directory.ts";
import * as encoding from "./encoding.ts";
import { Error_ } from "./error.ts";
import { File, file } from "./file.ts";
import { Graph, graph } from "./graph.ts";
import { error, log } from "./log.ts";
import type { Module } from "./module.ts";
import { Mutation, mutation } from "./mutation.ts";
import { Object as Object_ } from "./object.ts";
import { path } from "./path.ts";
import { Process } from "./process.ts";
import type { Reference } from "./reference.ts";
import type { Referent } from "./referent.ts";
import type { Resolved, Unresolved } from "./resolve.ts";
import { resolve } from "./resolve.ts";
import { RunBuilder } from "./run.ts";
import { sleep } from "./sleep.ts";
import { start } from "./start.ts";
import { Symlink, symlink } from "./symlink.ts";
import type { Tag } from "./tag.ts";
import { Template, template } from "./template.ts";
import type {
	MaybeMutation,
	MaybeMutationMap,
	MaybeNestedArray,
	MaybePromise,
	MutationMap,
	ValueOrMaybeMutationMap,
} from "./util.ts";
import { Value } from "./value.ts";

let build = Process.build;
let run = Process.run;
let $ = run;

export type {
	ArchiveFormat,
	CompressionFormat,
	MaybeMutation,
	MaybeMutationMap,
	MaybeNestedArray,
	MaybePromise,
	Module,
	MutationMap,
	Reference,
	Referent,
	Resolved,
	Tag,
	Unresolved,
	ValueOrMaybeMutationMap,
};

export {
	Args,
	Artifact,
	Blob,
	BuildBuilder,
	Checksum,
	Command,
	CommandBuilder,
	Directory,
	Error_ as Error,
	File,
	Graph,
	Mutation,
	Object_ as Object,
	Process,
	RunBuilder,
	Symlink,
	Template,
	Value,
	$,
	archive,
	assert,
	blob,
	build,
	bundle,
	checksum,
	command,
	compress,
	decompress,
	directory,
	download,
	encoding,
	extract,
	file,
	graph,
	log,
	error,
	mutation,
	path,
	resolve,
	run,
	sleep,
	start,
	symlink,
	template,
	unimplemented,
	unreachable,
};
