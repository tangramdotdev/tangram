import * as tg from "@tangramdotdev/client";

export let log = (...args: Array<unknown>) => {
	let string = args
		.map((arg) => (typeof arg === "string" ? arg : stringify(arg)))
		.join(" ");
	syscall("log", "stdout", `${string}\n`);
};

export let error = (...args: Array<unknown>) => {
	let string = args
		.map((arg) => (typeof arg === "string" ? arg : stringify(arg)))
		.join(" ");
	syscall("log", "stderr", `${string}\n`);
};

let stringify = (value: unknown): string => {
	return stringifyInner(value, new WeakSet());
};

let stringifyInner = (value: unknown, visited: WeakSet<object>): string => {
	switch (typeof value) {
		case "string": {
			return `"${value}"`;
		}
		case "number": {
			return value.toString();
		}
		case "boolean": {
			return value ? "true" : "false";
		}
		case "undefined": {
			return "undefined";
		}
		case "object": {
			if (value === null) {
				return "null";
			} else {
				return stringifyObject(value, visited);
			}
		}
		case "function": {
			if (value instanceof tg.Command) {
				return stringifyObject(value, visited);
			} else {
				return `(function "${value.name ?? "(anonymous)"}")`;
			}
		}
		case "symbol": {
			return "(symbol)";
		}
		case "bigint": {
			return value.toString();
		}
	}
};

let stringifyObject = (value: object, visited: WeakSet<object>): string => {
	if (visited.has(value)) {
		return "(circular)";
	}
	visited.add(value);
	let output: string;
	if (value instanceof Error) {
		output = value.message;
	} else if (value instanceof Promise) {
		output = "(promise)";
	} else if (value instanceof tg.Blob) {
		output = stringifyState(value.state, visited);
	} else if (value instanceof tg.Directory) {
		output = stringifyState(value.state, visited);
	} else if (value instanceof tg.File) {
		output = stringifyState(value.state, visited);
	} else if (value instanceof tg.Symlink) {
		output = stringifyState(value.state, visited);
	} else if (value instanceof tg.Graph) {
		output = stringifyState(value.state, visited);
	} else if (value instanceof tg.Command) {
		output = stringifyState(value.state, visited);
	} else if (value instanceof Uint8Array) {
		let bytes = tg.encoding.hex.encode(value);
		output = `tg.bytes(${bytes})`;
	} else if (value instanceof tg.Mutation) {
		output = `tg.mutation(${stringifyObject(value.inner, visited)})`;
	} else if (value instanceof tg.Template) {
		output = `\`${value.components
			.map((component) => {
				if (typeof component === "string") {
					return component;
				} else {
					return `\${${stringifyInner(component, visited)}}`;
				}
			})
			.join("")}\``;
	} else if (value instanceof Array) {
		output = `[${value
			.map((value) => stringifyInner(value, visited))
			.join(", ")}]`;
	} else {
		let string = "";
		if (
			value.constructor !== undefined &&
			value.constructor.name !== "Object"
		) {
			string += `${value.constructor.name} `;
		}
		string += "{";
		let entries = Object.entries(value);
		if (entries.length > 0) {
			string += " ";
		}
		string += entries
			.map(([key, value]) => `"${key}": ${stringifyInner(value, visited)}`)
			.join(", ");
		if (entries.length > 0) {
			string += " ";
		}
		string += "}";
		output = string;
	}
	visited.delete(value);
	return output;
};

let stringifyState = (
	state: tg.Object.State,
	visited: WeakSet<object>,
): string => {
	let object = state.object;
	if (object === undefined) {
		return state.id;
	}
	return `tg.${object.kind}(${stringifyObject(object.value, visited)})`;
};
