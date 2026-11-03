import * as tg from "@tangramdotdev/client";

export let start = async (): Promise<tg.Value.Data> => {
	// Import the module.
	let specifier = tg.Referent.toDataString(
		tg.process.module.referent,
		tg.Module.Source.toDataString,
	);
	let attributes = { kind: tg.process.module.kind };
	let export_ = tg.process.export;
	let namespace = await import(specifier, { with: attributes });

	// If there is no export, then return null.
	if (export_ === null) {
		return null;
	}

	// Get the export and call it if it is a function.
	if (!(export_ in namespace)) {
		throw new Error(`failed to find the export named ${export_}`);
	}
	let value = await namespace[export_];
	if (typeof value === "function") {
		// A function that returns nothing produces the null value.
		let result = await value(...tg.process.args);
		value = result === undefined ? null : result;
	}
	let output = await tg.resolve(value);

	// Store the output and get its data.
	await tg.Value.store(output);
	let outputData = tg.Value.toData(output);

	return outputData;
};
