import { assert } from "./assert.ts";
import { Module } from "./module.ts";
import { resolve } from "./resolve.ts";
import { Symlink } from "./symlink.ts";
import { type Target, functions, setCurrentTarget } from "./target.ts";
import type { Value } from "./value.ts";

export let start = async (target: Target): Promise<Value> => {
	// Load the target.
	await target.load();

	// Set the current target.
	setCurrentTarget(target);

	// Load the executable.
	let lock = await target.lock();
	assert(lock !== undefined);
	let lockId = await lock.id();
	let executable = await target.executable();
	Symlink.assert(executable);
	let package_ = await executable.artifact();
	assert(package_ !== undefined);
	let packageId = await package_.id();
	let path = await executable.path();
	assert(path !== undefined);

	// Create the module.
	let kind: "js" | "ts";
	if (path.toString().endsWith(".js")) {
		kind = "js";
	} else {
		kind = "ts";
	}
	let module: Module = {
		kind,
		value: {
			kind: "package_artifact",
			value: {
				artifact: packageId,
				lock: lockId,
				path: path.toString(),
			},
		},
	};

	let url = Module.toUrl(module);
	await import(url);

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

	// Get the function.
	let function_ = functions[url]?.[name];
	if (!function_) {
		throw new Error("failed to find the function");
	}

	// Call the function.
	let output = await resolve(function_(...args.slice(1)));

	return output;
};
