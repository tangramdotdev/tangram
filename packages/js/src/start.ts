import * as tg from "@tangramdotdev/client";

type Arg = {
	args: Array<tg.Value.Data>;
	cwd: string;
	env: Record<string, tg.Value.Data>;
	executable: tg.Command.Data.Executable;
};

export let start = async (arg: Arg): Promise<tg.Value.Data> => {
	let executable = tg.Command.Executable.fromData(arg.executable);

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
		executable,
	});

	// Import the module.
	let specifier: string;
	let export_: string | undefined;
	if ("artifact" in executable) {
		specifier = executable.artifact.id;
		if (executable.path !== undefined) {
			specifier += `?path=${encodeURIComponent(executable.path)}`;
		}
	} else if ("module" in executable) {
		specifier = tg.Module.toDataString(executable.module);
		export_ = executable.export;
	} else if ("path" in executable) {
		specifier = executable.path;
	} else {
		return tg.unreachable();
	}
	let namespace = await import(specifier);

	// If there is no export, then return undefined.
	if (export_ === undefined) {
		return undefined;
	}

	// Call the export.
	let output: tg.Value;
	if (!(export_ in namespace)) {
		throw new Error(`failed to find the export named ${export_}`);
	}
	let value = await namespace[export_];
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
