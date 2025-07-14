import * as tg from "./index.ts";
import type { Range } from "./range.ts";

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
		// @ts-ignore
		globalThis.Error.captureStackTrace(arg, tg.error);
	}
	return new tg.Error(arg);
}

// biome-ignore lint/suspicious/noShadowRestrictedNames: <reason>
export class Error {
	code: string | undefined;
	location: tg.Error.Location | undefined;
	message: string | undefined;
	source: tg.Referent<tg.Error> | undefined;
	stack: Array<tg.Error.Location> | undefined;
	values: { [key: string]: string };

	constructor(arg: Error.Arg) {
		if ("code" in arg) {
			this.code = arg.code;
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
		location?: tg.Error.Location | undefined;
		message?: string;
		source?: tg.Referent<tg.Error> | undefined;
		stack?: Array<tg.Error.Location> | undefined;
		values?: { [key: string]: string } | undefined;
	};

	export type Location = {
		symbol?: string;
		file: File;
		range: Range;
	};

	export type File =
		| { kind: "internal"; value: string }
		| { kind: "module"; value: tg.Module };

	export type Data = {
		code?: string;
		location?: LocationData;
		message?: string;
		source?: tg.Referent<tg.Error.Data>;
		stack?: Array<LocationData>;
		values?: { [key: string]: string };
	};

	export type LocationData = {
		symbol?: string;
		file: FileData;
		range: Range;
	};

	export type FileData =
		| { kind: "internal"; value: string }
		| { kind: "module"; value: tg.Module.Data };

	export let toData = (value: Error): Data => {
		let data: tg.Error.Data = {};
		if (value.code !== undefined) {
			data.code = value.code;
		}
		if (value.location !== undefined) {
			data.location = Location.toData(value.location);
		}
		if (value.message !== undefined) {
			data.message = value.message;
		}
		if (value.source !== undefined) {
			data.source = {
				item: Error.toData(value.source.item),
			};
			if (value.source.path !== undefined) {
				data.source.path = value.source.path;
			}
			if (value.source.tag !== undefined) {
				data.source.tag = value.source.tag;
			}
		}
		if (value.stack !== undefined) {
			data.stack = value.stack.map(Location.toData);
		}
		if (value.values !== undefined) {
			data.values = value.values;
		}
		return data;
	};

	export let fromData = (data: Data): Error => {
		let arg: tg.Error.Arg = {};
		if ("code" in data) {
			arg.code = data.code;
		}
		if ("location" in data) {
			arg.location = Location.fromData(data.location);
		}
		if ("message" in data) {
			arg.message = data.message;
		}
		if ("source" in data) {
			arg.source = {
				...data.source,
				item: Error.fromData(data.source.item),
			};
		}
		if ("stack" in data) {
			arg.stack = data.stack.map(Location.fromData);
		}
		if ("values" in data) {
			arg.values = data.values;
		}
		return new tg.Error(arg);
	};

	export namespace Location {
		export let toData = (value: Location): LocationData => {
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

		export let fromData = (data: LocationData): Location => {
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
