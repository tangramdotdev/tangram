// The JS/TS model uses `undefined` exclusively; `null` exists only as a JSON wire token. These helpers
// translate between the two at the serialization boundary, preserving object keys so that a present-but-empty
// map entry survives the round trip.

export let undefinedToNull = (value: unknown): unknown => {
	if (value === undefined) {
		return null;
	} else if (Array.isArray(value)) {
		return value.map(undefinedToNull);
	} else if (isPlainObject(value)) {
		let output: { [key: string]: unknown } = {};
		for (let [key, entry] of Object.entries(value)) {
			output[key] = undefinedToNull(entry);
		}
		return output;
	} else {
		return value;
	}
};

export let nullToUndefined = <T>(value: unknown): T => {
	return replaceNull(value) as T;
};

let replaceNull = (value: unknown): unknown => {
	if (value === null) {
		return undefined;
	} else if (Array.isArray(value)) {
		return value.map(replaceNull);
	} else if (isPlainObject(value)) {
		let output: { [key: string]: unknown } = {};
		for (let [key, entry] of Object.entries(value)) {
			output[key] = replaceNull(entry);
		}
		return output;
	} else {
		return value;
	}
};

let isPlainObject = (value: unknown): value is { [key: string]: unknown } => {
	if (typeof value !== "object" || value === null) {
		return false;
	}
	let prototype = Object.getPrototypeOf(value);
	return prototype === Object.prototype || prototype === null;
};
