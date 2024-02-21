import { Args } from "./args.ts";
import { Artifact } from "./artifact.ts";
import { assert, unimplemented, unreachable } from "./assert.ts";
import { Blob, blob, download } from "./blob.ts";
import { Branch, branch } from "./branch.ts";
import { Checksum, checksum } from "./checksum.ts";
import { Directory, directory } from "./directory.ts";
import * as encoding from "./encoding.ts";
import { Error as Error_, prepareStackTrace } from "./error.ts";
import { File, file } from "./file.ts";
import { include } from "./include.ts";
import { Leaf, leaf } from "./leaf.ts";
import { Lock } from "./lock.ts";
import { log } from "./log.ts";
import { Mutation, mutation } from "./mutation.ts";
import { resolve } from "./resolve.ts";
import { sleep } from "./sleep.ts";
import { start } from "./start.ts";
import { Symlink, symlink } from "./symlink.ts";
import { Target, build, getCurrent, target } from "./target.ts";
import { Template, template } from "./template.ts";
import { Triple, triple } from "./triple.ts";
import { Value } from "./value.ts";

Object.defineProperties(Error, {
	prepareStackTrace: { value: prepareStackTrace },
});

Object.defineProperties(globalThis, {
	console: { value: { log } },
});

async function tg(
	strings: TemplateStringsArray,
	...placeholders: Args<Template.Arg>
): Promise<Template> {
	let components: Args<Template.Arg> = [];
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
	Symlink,
	Triple,
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
	include,
	leaf,
	log,
	mutation,
	resolve,
	sleep,
	start,
	symlink,
	target,
	template,
	triple,
	unimplemented,
	unreachable,
});

Object.defineProperties(tg, {
	current: { get: getCurrent },
});

Object.defineProperties(globalThis, {
	tg: { value: tg },
});
