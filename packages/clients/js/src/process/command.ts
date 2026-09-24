import * as tg from "../index.ts";

export let inheritOptions = (
	command: tg.Process.Data.Command,
	options: tg.Referent.Options,
): tg.Process.Data.Command => {
	let output = JSON.parse(JSON.stringify(command)) as tg.Process.Data.Command;
	output.executable = inheritReferent(output.executable, options);
	if (output.stdin !== undefined && output.stdin !== null) {
		output.stdin = inheritReferent(output.stdin, options);
	}
	for (let value of [
		...(output.args ?? []),
		...Object.values(output.env ?? {}),
	]) {
		inheritValue(value.value, options);
	}
	return output;
};

export let withoutLocationAndTokens = (
	command: tg.Process.Data.Command,
): tg.Process.Data.Command => ({
	...command,
	args: (command.args ?? []).map((value) => ({
		...value,
		value: tg.Value.Data.withoutLocationAndTokens(value.value),
	})),
	env: Object.fromEntries(
		Object.entries(command.env ?? {}).map(([key, value]) => [
			key,
			{ ...value, value: tg.Value.Data.withoutLocationAndTokens(value.value) },
		]),
	),
	executable: tg.Referent.toData(
		tg.Referent.withoutLocationAndTokens(
			tg.Referent.fromData(command.executable, (node) => node),
		),
		(node) => node,
	),
	stdin:
		command.stdin === undefined || command.stdin === null
			? null
			: tg.Referent.toData(
					tg.Referent.withoutLocationAndTokens(
						tg.Referent.fromData(command.stdin, (node) => node),
					),
					(node) => node,
				),
});

let inheritReferent = <T>(
	data: tg.Referent.Data<T>,
	options: tg.Referent.Options,
	resource?: string,
): tg.Referent.Data<T> => {
	let referent = tg.Referent.fromData(data, (node) => node);
	referent.options ??= {};
	referent.options.location ??= options.location ?? null;
	referent.options.tokens ??= {};
	resource ??=
		typeof referent.node === "string"
			? referent.node
			: typeof referent.node === "object" &&
				  referent.node !== null &&
				  "artifact" in referent.node &&
				  typeof referent.node.artifact === "string"
				? referent.node.artifact
				: undefined;
	tg.Authorization.Tokens.inherit(
		referent.options.tokens,
		options.tokens ?? {},
		resource,
	);
	return tg.Referent.toData(referent, (node) => node);
};

let inheritString = (data: string, options: tg.Referent.Options): string => {
	let referent = tg.Referent.fromDataString(data, (node) => node);
	referent.options ??= {};
	referent.options.location ??= options.location ?? null;
	referent.options.tokens ??= {};
	tg.Authorization.Tokens.inherit(
		referent.options.tokens,
		options.tokens ?? {},
		referent.node,
	);
	return tg.Referent.toDataString(referent, (node) => node);
};

let inheritValue = (
	value: tg.Value.Data,
	options: tg.Referent.Options,
): void => {
	if (Array.isArray(value)) {
		for (let child of value) inheritValue(child, options);
	} else if (value !== null && typeof value === "object") {
		switch (value.kind) {
			case "map":
				for (let child of Object.values(value.value))
					inheritValue(child, options);
				break;
			case "module": {
				const module = tg.Module.fromData(value.value);
				const resource = tg.Module.children(module)[0]?.id;
				value.value.referent = inheritReferent(
					value.value.referent,
					options,
					resource,
				);
				break;
			}
			case "object":
				value.value = inheritString(value.value, options);
				break;
			case "template":
				inheritTemplate(value.value, options);
				break;
			case "mutation": {
				let mutation = value.value;
				switch (mutation.kind) {
					case "append":
					case "prepend":
						for (let child of mutation.values) inheritValue(child, options);
						break;
					case "merge":
						for (let child of Object.values(mutation.value))
							inheritValue(child, options);
						break;
					case "set":
					case "set_if_unset":
						inheritValue(mutation.value, options);
						break;
					case "prefix":
					case "suffix":
						inheritTemplate(mutation.template, options);
						break;
					case "unset":
						break;
				}
				break;
			}
			case "bytes":
			case "placeholder":
				break;
		}
	}
};

let inheritTemplate = (
	template: tg.Template.Data,
	options: tg.Referent.Options,
): void => {
	for (let component of template.components) {
		if (component.kind === "artifact")
			component.value = inheritString(component.value, options);
	}
};
