import type * as tg from "./index.ts";

export let flatten = <T>(value: MaybeNestedArray<T>): Array<T> => {
	if (value instanceof Array) {
		// @ts-ignore
		return value.flat(Number.POSITIVE_INFINITY);
	} else {
		return [value];
	}
};

export type MaybeNestedArray<T> = T | Array<MaybeNestedArray<T>>;

export type MaybePromise<T> = T | Promise<T>;

export type MaybeMutation<T extends tg.Value = tg.Value> = T | tg.Mutation<T>;

export type MutationMap<
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
> = {
	[K in keyof T]?: tg.Mutation<T[K]>;
};

export type MaybeMutationMap<
	T extends { [key: string]: tg.Value } = { [key: string]: tg.Value },
> = {
	[K in keyof T]?: MaybeMutation<T[K]>;
};

export type ValueOrMaybeMutationMap<T extends tg.Value = tg.Value> = T extends
	| undefined
	| boolean
	| number
	| string
	| Object
	| Uint8Array
	| tg.Mutation
	| tg.Template
	| Array<infer _U extends tg.Value>
	? T
	: T extends { [key: string]: tg.Value }
		? MaybeMutationMap<T>
		: never;

export let trimMultilineString = (
	strings: TemplateStringsArray,
	...placeholders: Array<string>
): string => {
	// Concatenate the strings and placeholders.
	let string = "";
	let i = 0;
	while (i < placeholders.length) {
		string += strings[i];
		string += placeholders[i];
		i = i + 1;
	}
	string += strings[i];

	// Split the lines.
	let lines = string.split("\n");

	// Remove the first and last lines.
	if (lines.length > 2) {
		lines = lines.slice(1, -1);
	}

	// Get the number of leading tabs to remove.
	const leadingWhitespaceCount = Math.min(
		...lines
			.filter((line) => line.length > 0)
			.map((line) => line.search(/[^\t ]|$/)),
	);

	// Remove the leading tabs from each line and combine them with newlines.
	return lines.map((line) => line.slice(leadingWhitespaceCount)).join("\n");
};

function is_whitespace(char: string | undefined): boolean {
	if (char === undefined) {
		return false;
	}
	if (char.length !== 1) {
		throw new Error("input must be a single character");
	}
	return char[0] === " " || char[0] === "\t";
}

function trimLeadingWhitespace(str: string): string {
	let lastNewline = -1;

	for (let i = 0; i < str.length; i++) {
		if (str[i] === "\n") {
			lastNewline = i;
		} else if (!is_whitespace(str[i])) {
			break;
		}
	}

	return str.slice(lastNewline + 1);
}

function trimLeadingWhitespaceLimit(str: string, limit: number): string {
	let i = 0;

	while (i < str.length && limit > 0 && is_whitespace(str[i])) {
		i++;
		limit--;
	}

	return str.slice(i);
}

function trimTrailingWhitespace(str: string): string {
	let end = str.length;

	while (end > 0 && is_whitespace(str[end - 1])) {
		end--;
	}

	return str.slice(0, end);
}

function countLeadingWhitespace(str: string): number {
	let i = 0;

	while (i < str.length && is_whitespace(str[i])) {
		i++;
	}

	return i;
}

export let trimTemplateStrings = (strings: Array<string>): Array<string> => {
	// Remove the trailing whitespace.
	const trailingRemoved = strings.map((str, index, arr) => {
		if (index === 0) {
			// Strip the first string's whitespace through the last '\n'.
			return trimLeadingWhitespace(str);
		} else if (index === arr.length - 1) {
			// Strip the last string's whitespace.
			return trimTrailingWhitespace(str);
		}
		return str;
	});

	// Get the shortest leading whitespace length.
	const leadingWhitespaceCount = Math.min(
		...trailingRemoved
			.flatMap((str) => str.split("\n"))
			.filter((line) => line.length > 0)
			.map((line) => countLeadingWhitespace(line)),
	);

	// Cut the leading whitespace to size on each line.
	return trailingRemoved.map((str) =>
		str
			.split("\n")
			.map((line) => trimLeadingWhitespaceLimit(line, leadingWhitespaceCount))
			.join("\n"),
	);
};
