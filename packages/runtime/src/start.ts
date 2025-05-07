import * as tg from "./index.ts";

export let start = async (process: tg.Process): Promise<tg.Value> => {
	// Load the process and the command.
	await process.load();
	const command = process.state!.command;
	await command.load();

	// Set the current process.
	tg.Process.current = process;

	// Import the module.
	// @ts-ignore
	// biome-ignore lint/security/noGlobalEval:
	let namespace = await eval(`import("!")`);

	// Get the export.
	let executable = await command.executable();
	tg.assert("module" in executable);
	let export_ = executable.export;
	if (export_ === undefined) {
		throw new Error("the executable must have an export");
	}

	// Get the output.
	let output: tg.Value;
	if (!(export_ in namespace)) {
		throw new Error("failed to find the export");
	}
	let value = await namespace[export_];
	if (tg.Value.is(value)) {
		output = value;
	} else if (typeof value === "function") {
		let args = await command.args();
		output = await tg.resolve(value(...args));
	} else {
		throw new Error("the export must be a tg.Value or a function");
	}

	return output;
};
