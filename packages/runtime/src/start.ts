import { assert } from "./assert.ts";
import * as encoding from "./encoding.ts";
import { Module } from "./module.ts";
import { resolve } from "./resolve.ts";
import { Symlink } from "./symlink.ts";
import { Target, functions, setCurrentTarget } from "./target.ts";
import { Value } from "./value.ts";

export let start = async (target: Target): Promise<Value> => {
	// Set the current target.
	await target.load();
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
	let url = Module.toUrl({
		kind: "normal",
		value: { lock: lockId, package: packageId, path: path.toString() },
	});
	await import(url);

	// Get the target.
	let name = await target.name_();
	if (!name) {
		throw new Error("the target must have a name");
	}

	// Get the function.
	let key = encoding.json.encode({ url, name });
	let function_ = functions[key];
	if (!function_) {
		throw new Error("failed to find the function");
	}

	// Get the args.
	let args = await target.args();

	// Call the function.
	let output = await resolve(function_(...args));

	return output;
};
