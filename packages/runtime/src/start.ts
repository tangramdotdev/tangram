import * as tg from "./index.ts";
import { setProcess } from "./process.ts";

export let start = async (process: tg.Process): Promise<tg.Value> => {
	// Set the process.
	await setProcess(process);

	// @ts-ignore
	// biome-ignore lint/security/noGlobalEval: special import
	let namespace = await eval(`import("!")`);

	// Get the command name.
	if (tg.process.args.length < 1) {
		throw new Error("the command must have at least one argument");
	}
	let name = tg.process.args.at(0);
	if (typeof name !== "string") {
		throw new Error("the command's first argument must be a string");
	}

	// Get the command.
	let value = namespace[name];
	let function_: Function;
	if (value instanceof tg.Command) {
		function_ = value.function()!;
	} else if (typeof value === "function") {
		function_ = value;
	} else {
		throw new Error("invalid export");
	}

	// Call the function and resolve its output.
	let output = await tg.resolve(function_!(...tg.process.args.slice(1)));

	return output;
};
