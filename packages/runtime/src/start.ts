import * as tg from "./index.ts";
import { Module } from "./module.ts";
import { setCurrentTarget } from "./target.ts";

export let start = async (target: tg.Target): Promise<tg.Value> => {
	// Load the target.
	await target.load();

	// Set the current target.
	setCurrentTarget(target);

	// Create the module for the executable.
	let executable = await target.executable();
	if (executable === undefined) {
		throw new Error("invalid target");
	}
	let kind = "js" as const;
	let object: string;
	let path: string | undefined;
	if (executable instanceof tg.File) {
		object = await executable.id();
	} else if (executable instanceof tg.Symlink) {
		let artifact = await executable.artifact();
		tg.assert(artifact);
		object = await artifact.id();
		path = (await executable.path())?.toString();
	} else {
		throw new Error("invalid executable");
	}
	let module = Module.print({
		path: {
			kind,
			object,
			path,
		},
	});

	// Import the module.
	let namespace = await import(module);

	// Get the args.
	let args = await target.args();

	// Get the target name.
	if (args.length < 1) {
		throw new Error("the target must have at least one argument");
	}
	let name = args.at(0);
	if (typeof name !== "string") {
		throw new Error("the target's first argument must be a string");
	}

	// Get the target.
	let target_ = namespace[name];
	if (!(target_ instanceof tg.Target)) {
		throw new Error(`failed to find the export named "${name}"`);
	}

	// Call the function and resolve its output.
	let output = await tg.resolve(target_.function()!(...args.slice(1)));

	return output;
};
