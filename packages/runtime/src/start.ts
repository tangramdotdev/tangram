import { setCurrentCommand } from "./command.ts";
import * as tg from "./index.ts";

export let start = async (command: tg.Command): Promise<tg.Value> => {
	// Load the command.
	await command.load();

	// Set the current command.
	setCurrentCommand(command);

	// @ts-ignore
	// biome-ignore lint/security/noGlobalEval: special import
	let namespace = await eval(`import("!")`);

	// Get the args.
	let args = await command.args();

	// Get the command name.
	if (args.length < 1) {
		throw new Error("the command must have at least one argument");
	}
	let name = args.at(0);
	if (typeof name !== "string") {
		throw new Error("the command's first argument must be a string");
	}

	// Get the command.
	let command_ = namespace[name];
	if (!(command_ instanceof tg.Command)) {
		throw new Error(`failed to find the export named "${name}"`);
	}

	// Call the function and resolve its output.
	let output = await tg.resolve(command_.function()!(...args.slice(1)));

	return output;
};
