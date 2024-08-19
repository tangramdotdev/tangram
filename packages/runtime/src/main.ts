import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert, unimplemented, unreachable } from "./assert.ts";
import { Blob, blob } from "./blob.ts";
import { Branch, branch } from "./branch.ts";
import { Checksum, checksum } from "./checksum.ts";
import { Directory, directory } from "./directory.ts";
import * as encoding from "./encoding.ts";
import { Error_ } from "./error.ts";
import { File, file } from "./file.ts";
import { Graph, graph } from "./graph.ts";
import { Leaf, leaf } from "./leaf.ts";
import { log } from "./log.ts";
import { Mutation, mutation } from "./mutation.ts";
import { path, Path } from "./path.ts";
import { resolve } from "./resolve.ts";
import { sleep } from "./sleep.ts";
import { start } from "./start.ts";
import { Symlink, symlink } from "./symlink.ts";
import { Target, getCurrentTarget, target } from "./target.ts";
import { Template, template } from "./template.ts";
import { Value } from "./value.ts";

let console = { log };
Object.defineProperties(globalThis, {
	console: {
		value: console,
		configurable: true,
		enumerable: true,
		writable: true,
	},
});

async function tg(
	strings: TemplateStringsArray,
	...placeholders: Args<Template.Arg>
): Promise<Template> {
	let components = [];
	for (let i = 0; i < strings.length - 1; i++) {
		let string = strings[i]!;
		components.push(string);
		let placeholder = placeholders[i]!;
		components.push(placeholder);
	}
	components.push(strings[strings.length - 1]!);
	return await template(...components);
}

let archive = Artifact.archive;
let bundle = Artifact.bundle;
let compress = Blob.compress;
let decompress = Blob.decompress;
let download = Blob.download;
let extract = Artifact.extract;

Object.assign(tg, {
	Args,
	Artifact,
	Blob,
	Branch,
	Checksum,
	Directory,
	Error: Error_,
	File,
	Leaf,
	Graph,
	Mutation,
	Path,
	Symlink,
	Target,
	Template,
	Value,
	archive,
	assert,
	blob,
	branch,
	bundle,
	checksum,
	compress,
	decompress,
	directory,
	download,
	encoding,
	extract,
	file,
	leaf,
	graph,
	log,
	mutation,
	path,
	resolve,
	sleep,
	start,
	symlink,
	target,
	template,
	unimplemented,
	unreachable,
});

Object.defineProperties(tg, {
	current: { get: getCurrentTarget },
});

Object.defineProperties(globalThis, {
	tg: { value: tg },
});
