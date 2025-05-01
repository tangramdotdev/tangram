import * as tg from "./index.ts";

export let start = async (process: tg.Process): Promise<tg.Value> => {
	// Load the process and the command.
	await process.load();
	const command = process.state!.command;
	await command.load();

	// Set the current process.
	tg.Process.current = process;

	// @ts-ignore
	// biome-ignore lint/security/noGlobalEval: special import
	let namespace = await eval(`import("!")`);

	// Get the target.
	let executable = await command.executable();
	tg.assert("module" in executable);
	let target = executable.target;
	if (target === undefined) {
		throw new Error("the executable must have a target");
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
	let args = await command.args();
	let output = await tg.resolve(function_!(...args));

	return output;
};
