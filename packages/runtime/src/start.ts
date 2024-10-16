import * as tg from "./index.ts";
import type { Module } from "./module.ts";
import { setCurrentTarget } from "./target.ts";

export let start = async (target: tg.Target): Promise<tg.Value> => {
	// Load the target.
	await target.load();

	// Set the current target.
	setCurrentTarget(target);

	// Import the executable.
	let executable = await target.executable();
	if (executable === undefined) {
		throw new Error("invalid target");
	}
	let kind: Module.Kind = "js";
	if (executable instanceof tg.Symlink) {
		const path = await executable.path();
		if (path?.toString().endsWith(".ts")) {
			kind = "ts";
		}
	}
	let id = await executable.id();

	// biome-ignore lint/security/noGlobalEval: workaround for https://github.com/oven-sh/bun/issues/14064.
	let namespace = await eval(`import("${id}", { with: { kind: "${kind}" } })`);

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
