import { assert } from "./assert.ts";
import { Module } from "./module.ts";
import { resolve } from "./resolve.ts";
import { Target, setCurrentTarget } from "./target.ts";
import type { Value } from "./value.ts";

export let start = async (target: Target): Promise<Value> => {
	// Load the target.
	await target.load();

	// Set the current target.
	setCurrentTarget(target);

	// Create the module.
	let executable = await target.executable();
	if (executable === undefined) {
		throw new Error("invalid target");
	}
	let object = await executable.object();
	let metadata = object.nodes[object.root]!.metadata;
	if (!("kind" in metadata)) {
		throw new Error("the kind must be set");
	}
	assert(
		metadata.kind === ("js" as const) || metadata.kind === ("ts" as const),
		"invalid kind",
	);
	let module = {
		kind: metadata.kind,
		object: await executable.id(),
	};

	// Create the URL.
	let url = Module.toUrl(module);

	// Import the module.
	let namespace = await import(url);

	// Get the args.
	let args = await target.args();

	// Get the target name.
	if (args.length < 1) {
		throw new Error("the target must have at least one arg");
	}
	let name = args.at(0);
	if (typeof name !== "string") {
		throw new Error("the target's first arg must be a string");
	}

	// Get the target.
	let target_ = namespace[name];
	if (!(target_ instanceof Target)) {
		throw new Error(`failed to find the export named "${name}"`);
	}

	// Call the function.
	let output = await resolve(target_.function()!(...args.slice(1)));

	return output;
};
