import * as tg from "./index.ts";

export function error(): tg.Error;
export function error(message: string, arg?: tg.Error.Arg): tg.Error;
export function error(arg: tg.Error.Arg): tg.Error;
export function error(
	firstArg?: string | Error.Arg,
	secondArg?: Error.Arg,
): tg.Error {
	let arg: tg.Error.Arg = {};
	if (firstArg !== undefined && typeof firstArg === "string") {
		arg.message = firstArg;
		if (secondArg !== undefined) {
			if ("code" in secondArg) {
				arg.code = secondArg.code;
			}
			if ("diagnostics" in secondArg) {
				arg.diagnostics = secondArg.diagnostics;
			}
			if ("location" in secondArg) {
				arg.location = secondArg.location;
			}
			if ("message" in secondArg) {
				arg.message = secondArg.message;
			}
			if ("source" in secondArg) {
				arg.source = secondArg.source;
			}
			if ("stack" in secondArg) {
				arg.stack = secondArg.stack;
			}
			if ("values" in secondArg) {
				arg.values = secondArg.values ?? {};
			}
		}
	} else if (firstArg !== undefined && typeof firstArg === "object") {
		if ("code" in firstArg) {
			arg.code = firstArg.code;
		}
		if ("diagnostics" in firstArg) {
			arg.diagnostics = firstArg.diagnostics;
		}
		if ("location" in firstArg) {
			arg.location = firstArg.location;
		}
		if ("message" in firstArg) {
			arg.message = firstArg.message;
		}
		if ("source" in firstArg) {
			arg.source = firstArg.source;
		}
		if ("stack" in firstArg) {
			arg.stack = firstArg.stack;
		}
		if ("values" in firstArg) {
			arg.values = firstArg.values ?? {};
		}
	}
	if (!("stack" in arg)) {
		// @ts-expect-error
		globalThis.Error.captureStackTrace(arg, tg.error);
	}
	return new tg.Error(arg);
}

// biome-ignore lint/suspicious/noShadowRestrictedNames: <reason>
export class Error {
	code: string | undefined;
	diagnostics: Array<tg.Diagnostic> | undefined;
	location: tg.Error.Location | undefined;
	message: string | undefined;
	source: tg.Referent<tg.Error> | undefined;
	stack: Array<tg.Error.Location> | undefined;
	values: { [key: string]: string };

	constructor(arg: Error.Arg) {
		if ("code" in arg) {
			this.code = arg.code;
		}
		if ("diagnostics" in arg) {
			this.diagnostics = arg.diagnostics;
		}
		if ("location" in arg) {
			this.location = arg.location;
		}
		if ("message" in arg) {
			this.message = arg.message;
		}
		if ("source" in arg) {
			this.source = arg.source;
		}
		if ("stack" in arg) {
			this.stack = arg.stack;
		}
		if ("values" in arg) {
			this.values = arg.values ?? {};
		}
		this.values ??= {};
	}
}

export namespace Error {
	export type Arg = {
		code?: string | undefined;
		diagnostics?: Array<tg.Diagnostic> | undefined;
		location?: tg.Error.Location | undefined;
		message?: string;
		source?: tg.Referent<tg.Error> | undefined;
		stack?: Array<tg.Error.Location> | undefined;
		values?: { [key: string]: string } | undefined;
	};

	export type Location = {
		symbol?: string;
		file: tg.Error.File;
		range: tg.Range;
	};

	export type File =
		| { kind: "internal"; value: string }
		| { kind: "module"; value: tg.Module };

	export type Data = {
		code?: string;
		diagnostics?: Array<tg.Diagnostic.Data>;
		location?: tg.Error.Data.Location;
		message?: string;
		source?: tg.Referent.Data<tg.Error.Data>;
		stack?: Array<tg.Error.Data.Location>;
		values?: { [key: string]: string };
	};

	export namespace Data {
		export type Location = {
			symbol?: string;
			file: tg.Error.Data.File;
			range: tg.Range;
		};

		export type File =
			| { kind: "internal"; value: string }
			| { kind: "module"; value: tg.Module.Data };
	}

	export let toData = (value: tg.Error): tg.Error.Data => {
		let data: tg.Error.Data = {};
		if (value.code !== undefined) {
			data.code = value.code;
		}
		if (value.diagnostics !== undefined) {
			data.diagnostics = value.diagnostics.map(tg.Diagnostic.toData);
		}
		if (value.location !== undefined) {
			data.location = tg.Error.Location.toData(value.location);
		}
		if (value.message !== undefined) {
			data.message = value.message;
		}
		if (value.source !== undefined) {
			data.source = tg.Referent.toData(value.source, tg.Error.toData);
		}
		if (value.stack !== undefined) {
			data.stack = value.stack.map(tg.Error.Location.toData);
		}
		if (value.values !== undefined) {
			data.values = value.values;
		}
		return data;
	};

	export let fromData = (data: tg.Error.Data): tg.Error => {
		let arg: tg.Error.Arg = {};
		if ("code" in data) {
			arg.code = data.code;
		}
		if ("diagnostics" in data) {
			arg.diagnostics = data.diagnostics?.map(tg.Diagnostic.fromData);
		}
		if ("location" in data) {
			arg.location = tg.Error.Location.fromData(data.location);
		}
		if ("message" in data) {
			arg.message = data.message;
		}
		if ("source" in data) {
			arg.source = tg.Referent.fromData(data.source, tg.Error.fromData);
		}
		if ("stack" in data) {
			arg.stack = data.stack.map(tg.Error.Location.fromData);
		}
		if ("values" in data) {
			arg.values = data.values;
		}
		return new tg.Error(arg);
	};

	export namespace Location {
		export let toData = (value: tg.Error.Location): tg.Error.Data.Location => {
			let file =
				value.file.kind === "module"
					? {
							kind: "module" as const,
							value: tg.Module.toData(value.file.value),
						}
					: value.file;
			return {
				...value,
				file: file,
			};
		};

		export let fromData = (data: tg.Error.Data.Location): tg.Error.Location => {
			let file =
				data.file.kind === "module"
					? {
							kind: "module" as const,
							value: tg.Module.fromData(data.file.value),
						}
					: data.file;
			return {
				...data,
				file,
			};
		};
	}
}
