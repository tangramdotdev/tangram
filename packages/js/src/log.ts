import * as tg from "@tangramdotdev/client";

export let log = (...args: Array<unknown>) => {
	let string = args
		.map((arg) =>
			typeof arg === "string" ? arg : stringify(arg, tg.host.isTty(1)),
		)
		.join(" ");
	tg.host.writeSync(1, tg.encoding.utf8.encode(`${string}\n`));
};

export let error = (...args: Array<unknown>) => {
	let string = args
		.map((arg) =>
			typeof arg === "string" ? arg : stringify(arg, tg.host.isTty(2)),
		)
		.join(" ");
	tg.host.writeSync(2, tg.encoding.utf8.encode(`${string}\n`));
};

let colors = {
	reset: "\x1b[0m",
	gray: "\x1b[37m",
	cyan: "\x1b[96m",
	yellow: "\x1b[93m",
	green: "\x1b[32m",
	blue: "\x1b[94m",
};

let highlight = false;
let visited = new WeakSet<object>();

let stringify = (value: unknown, highlight_ = false): string => {
	highlight = highlight_;
	visited = new WeakSet();
	try {
		return stringifyInner(value);
	} finally {
		highlight = false;
		visited = new WeakSet();
	}
};

let stringifyInner = (value: unknown): string => {
	switch (typeof value) {
		case "string": {
			return style(JSON.stringify(value), colors.green);
		}
		case "number": {
			return style(value.toString(), colors.yellow);
		}
		case "boolean": {
			return style(value ? "true" : "false", colors.yellow);
		}
		case "undefined": {
			return style("undefined", colors.gray);
		}
		case "object": {
			if (value === null) {
				return style("null", colors.gray);
			} else {
				return stringifyObject(value);
			}
		}
		case "function": {
			if (value instanceof tg.Command) {
				return stringifyObject(value);
			} else {
				return style(
					`(function ${JSON.stringify(value.name ?? "(anonymous)")})`,
					colors.cyan,
				);
			}
		}
		case "symbol": {
			return style("(symbol)", colors.cyan);
		}
		case "bigint": {
			return style(value.toString(), colors.yellow);
		}
	}
};

let stringifyObject = (value: object): string => {
	if (visited.has(value)) {
		return style("(circular)", colors.cyan);
	}
	visited.add(value);
	let output: string;
	if (value instanceof Error) {
		output = value.message;
	} else if (value instanceof Promise) {
		output = style("(promise)", colors.cyan);
	} else if (value instanceof tg.Blob) {
		output = stringifyState(value.state);
	} else if (value instanceof tg.Directory) {
		output = stringifyState(value.state);
	} else if (value instanceof tg.File) {
		output = stringifyState(value.state);
	} else if (value instanceof tg.Symlink) {
		output = stringifyState(value.state);
	} else if (value instanceof tg.Graph) {
		output = stringifyState(value.state);
	} else if (value instanceof tg.Command) {
		output = stringifyState(value.state);
	} else if (value instanceof tg.Placeholder) {
		output = call(
			"placeholder",
			style(JSON.stringify(value.name), colors.green),
		);
	} else if (value instanceof Uint8Array) {
		let bytes = tg.encoding.hex.encode(value);
		output = call("bytes", style(bytes, colors.yellow));
	} else if (value instanceof tg.Mutation) {
		output = call("mutation", stringifyObject(value.inner));
	} else if (value instanceof tg.Template) {
		output = stringifyTemplate(value);
	} else if (value instanceof Array) {
		output = `${style("[")}${value
			.map((value) => stringifyInner(value))
			.join(`${style(",")} `)}${style("]")}`;
	} else {
		let string = "";
		let prototype = Object.getPrototypeOf(value);
		let constructorName =
			prototype === null
				? undefined
				: Object.getOwnPropertyDescriptor(prototype, "constructor")?.value
						?.name;
		if (constructorName !== undefined && constructorName !== "Object") {
			string += `${style(constructorName, colors.blue)} `;
		}
		string += style("{");
		let entries = Object.entries(
			Object.getOwnPropertyDescriptors(value),
		).filter(([, descriptor]) => descriptor.enumerable);
		if (entries.length > 0) {
			string += " ";
		}
		string += entries
			.map(
				([key, descriptor]) =>
					`${style(JSON.stringify(key), colors.green)}${style(":")} ${stringifyProperty(descriptor)}`,
			)
			.join(`${style(",")} `);
		if (entries.length > 0) {
			string += " ";
		}
		string += style("}");
		output = string;
	}
	visited.delete(value);
	return output;
};

let stringifyState = (state: tg.Object.State): string => {
	let object = state.object;
	if (object === undefined) {
		return style(state.id, colors.gray);
	}
	return call(object.kind, stringifyObject(object.value));
};

let stringifyTemplate = (value: tg.Template): string => {
	return `${style("tg")}${style("`", colors.green)}${value.components
		.map((component) => {
			if (typeof component === "string") {
				return style(escapeTemplateString(component), colors.green);
			} else {
				return `${style("${")}${stringifyInner(component)}${style("}")}`;
			}
		})
		.join("")}${style("`", colors.green)}`;
};

let escapeTemplateString = (value: string): string => {
	return value
		.replaceAll("\\", "\\\\")
		.replaceAll("`", "\\`")
		.replaceAll("${", "\\${");
};

let call = (name: string, arg: string): string => {
	return `${style("tg")}${style(".")}${style(name, colors.blue)}${style("(")}${arg}${style(")")}`;
};

let stringifyProperty = (descriptor: PropertyDescriptor): string => {
	if ("value" in descriptor) {
		return stringifyInner(descriptor.value);
	}
	if (descriptor.get !== undefined && descriptor.set !== undefined) {
		return style("(accessor)", colors.cyan);
	}
	if (descriptor.get !== undefined) {
		return style("(getter)", colors.cyan);
	}
	if (descriptor.set !== undefined) {
		return style("(setter)", colors.cyan);
	}
	return style("(accessor)", colors.cyan);
};

let style = (value: string, code?: string | undefined): string => {
	return highlight && code !== undefined
		? `${code}${value}${colors.reset}`
		: value;
};
