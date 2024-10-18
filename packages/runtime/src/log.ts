import * as tg from "./index.ts";

export let log = (...args: Array<unknown>) => {
	let string = args.map((arg) => stringify(arg)).join(" ");
	syscall("log", `${string}\n`);
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
			if (value instanceof tg.Target) {
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
	} else if (value instanceof tg.Leaf) {
		output = stringifyState("leaf", value.state, visited);
	} else if (value instanceof tg.Branch) {
		output = stringifyState("branch", value.state, visited);
	} else if (value instanceof tg.Directory) {
		output = stringifyState("directory", value.state, visited);
	} else if (value instanceof tg.File) {
		output = stringifyState("file", value.state, visited);
	} else if (value instanceof tg.Symlink) {
		output = stringifyState("symlink", value.state, visited);
	} else if (value instanceof tg.Graph) {
		output = stringifyState("graph", value.state, visited);
	} else if (value instanceof tg.Target) {
		output = stringifyState("target", value.state, visited);
	} else if (value instanceof Uint8Array) {
		let bytes = tg.encoding.hex.encode(value);
		output = `(bytes ${bytes})`;
	} else if (value instanceof tg.Mutation) {
		output = `(mutation ${stringifyObject(value.inner, visited)})`;
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
	kind: string,
	state: tg.Object.State<string, object>,
	visited: WeakSet<object>,
): string => {
	let { id, object } = state;
	if (id !== undefined) {
		return id;
	} else if (object !== undefined) {
		return `(${kind} ${stringifyObject(object, visited)})`;
	} else {
		return tg.unreachable();
	}
};
