import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert, unimplemented, unreachable } from "./assert.ts";
import { Blob, blob, download } from "./blob.ts";
import { Branch, branch } from "./branch.ts";
import { Checksum, checksum } from "./checksum.ts";
import { Directory, directory } from "./directory.ts";
import * as encoding from "./encoding.ts";
import { Error_ } from "./error.ts";
import { File, file } from "./file.ts";
import { Leaf, leaf } from "./leaf.ts";
import { Lock, lock } from "./lock.ts";
import { log } from "./log.ts";
import { Mutation, mutation } from "./mutation.ts";
import { path, Path } from "./path.ts";
import { type Unresolved, resolve } from "./resolve.ts";
import { sleep } from "./sleep.ts";
import { start } from "./start.ts";
import { Symlink, symlink } from "./symlink.ts";
import { Target, build, getCurrentTarget, target } from "./target.ts";
import { Template, template } from "./template.ts";
import type { MaybeNestedArray } from "./util.ts";
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
	...placeholders: Array<Unresolved<MaybeNestedArray<Template.Arg>>>
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
	Lock,
	Mutation,
	Path,
	Symlink,
	Target,
	Template,
	Value,
	assert,
	blob,
	branch,
	build,
	checksum,
	directory,
	download,
	encoding,
	file,
	leaf,
	lock,
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
