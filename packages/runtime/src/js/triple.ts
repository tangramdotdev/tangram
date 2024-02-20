import { assert as assert_ } from "./assert.ts";

export let triple = (...args: Array<Triple.Arg>): Triple => {
	let arch: Triple.Arch | undefined = undefined;
	let os: Triple.Os | undefined = undefined;
	args.forEach((arg) => {
		if (Triple.is(arg)) {
			arch = Triple.arch(arg);
			os = Triple.os(arg);
		} else {
			if (arg.arch !== undefined) {
				arch = arg.arch;
			}
			if (arg.os !== undefined) {
				os = arg.os;
			}
		}
	});
	assert_(arch !== undefined, "arch must be defined.");
	assert_(os !== undefined, "os must be defined.");
	return `${arch}-${os}` as Triple;
};

export type Triple =
	| "aarch64-darwin"
	| "aarch64-linux"
	| "js-js"
	| "x86_64-darwin"
	| "x86_64-linux";

export declare namespace Triple {
	let new_: (...args: Array<Triple.Arg>) => Triple;
	export { new_ as new };
}

export namespace Triple {
	export type Arg = Triple | ArgObject;

	export type ArgObject = {
		arch?: Arch;
		os?: Os;
	};

	export type Arch = "aarch64" | "js" | "x86_64";

	export type Os = "darwin" | "js" | "linux";
	export let new_ = (...args: Array<Triple.Arg>): Triple => {
		return triple(...args);
	};
	Triple.new = new_;

	export let is = (value: unknown): value is Triple => {
		return (
			value === "aarch64-darwin" ||
			value === "aarch64-linux" ||
			value === "js-js" ||
			value === "x86_64-darwin" ||
			value === "x86_64-linux"
		);
	};

	export let expect = (value: unknown): Triple => {
		assert_(Triple.is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Triple => {
		assert_(Triple.is(value));
	};

	export let arch = (system: Triple): Arch => {
		switch (system) {
			case "aarch64-darwin":
			case "aarch64-linux": {
				return "aarch64";
			}
			case "js-js": {
				return "js";
			}
			case "x86_64-linux":
			case "x86_64-darwin": {
				return "x86_64";
			}
			default: {
				throw new Error("Invalid system.");
			}
		}
	};

	export let os = (system: Triple): Os => {
		switch (system) {
			case "aarch64-darwin":
			case "x86_64-darwin": {
				return "darwin";
			}
			case "js-js": {
				return "js";
			}
			case "x86_64-linux":
			case "aarch64-linux": {
				return "linux";
			}
			default: {
				throw new Error("Invalid system.");
			}
		}
	};
}
