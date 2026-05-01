import * as tg from "@tangramdotdev/client";

export let start = async (): Promise<tg.Value.Data> => {
	// Import the module.
	let specifier: string;
	let attributes: { [key: string]: string } = {};
	let export_: string | undefined;
	if ("artifact" in tg.process.executable) {
		specifier = tg.process.executable.artifact.id;
		if (tg.process.executable.path !== undefined) {
			specifier += `?get=${encodeURIComponent(tg.process.executable.path)}`;
		}
	} else if ("module" in tg.process.executable) {
		specifier = tg.Referent.toDataString(
			tg.process.executable.module.referent,
			tg.Module.Item.toDataString,
		);
		attributes.kind = tg.process.executable.module.kind;
		export_ = tg.process.executable.export;
	} else if ("path" in tg.process.executable) {
		specifier = tg.process.executable.path;
	} else {
		return tg.unreachable();
	}
	let namespace = await import(specifier, { with: attributes });

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
