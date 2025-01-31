import * as tg from "./index.ts";
import { setProcess } from "./process.ts";

export let start = async (process: tg.Process): Promise<tg.Value> => {
	// Load the process and command.
	await process.load();
	const command = await process.command();
	await command.load();

	// Set the process.
	await setProcess(process);

	// @ts-ignore
	// biome-ignore lint/security/noGlobalEval: special import
	let namespace = await eval(`import("!")`);

	// Get the target.
	const args = await command.args();
	if (args.length < 1) {
		throw new Error("the command must have at least one argument");
	}
	let target = args.at(0);
	if (typeof target !== "string") {
		throw new Error("the command's first argument must be a string");
	}

	// Get the command.
	let value = namespace[target];
	let function_: Function;
	if (value instanceof tg.Command) {
		function_ = value.function()!;
	} else if (typeof value === "function") {
		function_ = value;
	} else {
		throw new Error("invalid export");
	}

	// Call the function and resolve its output.
	let output = await tg.resolve(function_!(...args.slice(1)));

	return output;
};
