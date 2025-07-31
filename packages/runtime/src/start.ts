import * as tg from "./index.ts";

export let start = async (process: tg.Process): Promise<tg.Value.Data> => {
	// Set the current process.
	tg.Process.current = process;

	// Load the process.
	await process.load();

	// Load the command.
	let command = await process.command();
	await command.load();

	// Import the module.
	// @ts-ignore
	// biome-ignore lint/security/noGlobalEval: <reason>
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
		throw new Error(`failed to find the export: ${command.id}`);
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

	// Store the output.
	await tg.Value.store(output);

	// Get the output data.
	let outputData = tg.Value.toData(output);

	return outputData;
};
