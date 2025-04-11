import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert, unimplemented, unreachable } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import { Branch, branch } from "./branch.ts";
import { BuildBuilder } from "./build.ts";
import { Checksum, checksum } from "./checksum.ts";
import { Command, CommandBuilder, command } from "./command.ts";
import { Directory, directory } from "./directory.ts";
import * as encoding from "./encoding.ts";
import { Error_ } from "./error.ts";
import { File, file } from "./file.ts";
import { Graph, graph } from "./graph.ts";
import { Leaf, leaf } from "./leaf.ts";
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

let archive = Artifact.archive;
let build = Process.build;
let bundle = Artifact.bundle;
let compress = Blob.compress;
let decompress = Blob.decompress;
let download = (
	url: string,
	unpack: boolean,
	checksum: Checksum,
): Promise<Blob | Artifact> => {
	if (unpack) {
		return Artifact.download(url, checksum);
	} else {
		return Blob.download(url, checksum);
	}
};
let extract = Artifact.extract;
let run = Process.run;
let $ = run;

export {
	Args,
	Artifact,
	Blob,
	Branch,
	BuildBuilder,
	Checksum,
	Command,
	CommandBuilder,
	Directory,
	Error_ as Error,
	File,
	Graph,
	Leaf,
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
	branch,
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
	leaf,
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
	type MaybeMutation,
	type MaybeMutationMap,
	type MaybeNestedArray,
	type MaybePromise,
	type Module,
	type MutationMap,
	type Reference,
	type Referent,
	type Resolved,
	type Tag,
	type Unresolved,
	type ValueOrMaybeMutationMap,
	unimplemented,
	unreachable,
};
