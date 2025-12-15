import * as tg from "@tangramdotdev/client";

type Arg = {
	args: Array<tg.Value.Data>;
	cwd: string;
	env: Record<string, tg.Value.Data>;
	executable: tg.Command.Data.Executable;
};

export let start = async (arg: Arg): Promise<tg.Value.Data> => {
	// Set tg.process.
	tg.setProcess({
		args: arg.args.map(tg.Value.fromData),
		cwd: arg.cwd,
		env: Object.fromEntries(
			Object.entries(arg.env).map(([key, value]) => [
				key,
				tg.Value.fromData(value),
			]),
		),
		executable: tg.Command.Executable.fromData(arg.executable),
	});

	// Set tg.Process.current from the TANGRAM_PROCESS environment variable if it is defined.
	let id = arg.env.TANGRAM_PROCESS;
	if (id !== undefined) {
		tg.assert(typeof id === "string");
		tg.Process.current = new tg.Process({ id });
	}

	// Import the module.
	let namespace = await eval(`import("!")`);

	// If there is no export, then return undefined.
	if (!("export" in arg.executable && arg.executable.export !== undefined)) {
		return undefined;
	}

	// Call the export.
	let output: tg.Value;
	if (!(arg.executable.export in namespace)) {
		throw new Error(`failed to find the export named ${arg.executable.export}`);
	}
	let value = await namespace[arg.executable.export];
	if (tg.Value.is(value)) {
		output = value;
	} else if (typeof value === "function") {
		output = await tg.resolve(value(...tg.process.args));
	} else {
		throw new Error("the export must be a tg.Value or a function");
	}

	// Store the output and get its data.
	await tg.Value.store(output);
	let outputData = tg.Value.toData(output);

	return outputData;
};
