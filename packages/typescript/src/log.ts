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
			return `(function "${value.name ?? "(anonymous)"}")`;
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
	if (value instanceof Array) {
		output = `[${value
			.map((value) => stringifyInner(value, visited))
			.join(", ")}]`;
	} else if (value instanceof Error) {
		output = value.message;
	} else if (value instanceof Promise) {
		output = "(promise)";
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
