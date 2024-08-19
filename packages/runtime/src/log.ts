import { unreachable } from "./assert.ts";
import { Branch } from "./branch.ts";
import { Directory } from "./directory.ts";
import * as encoding from "./encoding.ts";
import { File } from "./file.ts";
import { Graph } from "./graph.ts";
import { Leaf } from "./leaf.ts";
import { Mutation } from "./mutation.ts";
import type { Object_ } from "./object.ts";
import { Path } from "./path.ts";
import { Symlink } from "./symlink.ts";
import { Target } from "./target.ts";
import { Template } from "./template.ts";

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
			if (value instanceof Target) {
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
	if (value instanceof Error) {
		return value.message;
	} else if (value instanceof Promise) {
		return "(promise)";
	} else if (value instanceof Leaf) {
		return stringifyState("leaf", value.state, visited);
	} else if (value instanceof Branch) {
		return stringifyState("branch", value.state, visited);
	} else if (value instanceof Directory) {
		return stringifyState("directory", value.state, visited);
	} else if (value instanceof File) {
		return stringifyState("file", value.state, visited);
	} else if (value instanceof Symlink) {
		return stringifyState("symlink", value.state, visited);
	} else if (value instanceof Graph) {
		return stringifyState("graph", value.state, visited);
	} else if (value instanceof Target) {
		return stringifyState("target", value.state, visited);
	} else if (value instanceof Uint8Array) {
		let bytes = encoding.hex.encode(value);
		return `(bytes ${bytes})`;
	} else if (value instanceof Path) {
		return `(path "${value}")`;
	} else if (value instanceof Mutation) {
		return `(mutation ${stringifyObject(value.inner, visited)})`;
	} else if (value instanceof Template) {
		return `\`${value.components
			.map((component) => {
				if (typeof component === "string") {
					return component;
				} else {
					return `\${${stringifyInner(component, visited)}}`;
				}
			})
			.join("")}\``;
	} else if (value instanceof Array) {
		return `[${value
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
		return string;
	}
};

let stringifyState = (
	kind: string,
	state: Object_.State<string, object>,
	visited: WeakSet<object>,
): string => {
	let { id, object } = state;
	if (id !== undefined) {
		return id;
	} else if (object !== undefined) {
		return `(${kind} ${stringifyObject(object, visited)})`;
	} else {
		return unreachable();
	}
};
