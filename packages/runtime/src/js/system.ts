import { assert as assert_ } from "./assert.ts";

export let system = (...args: Array<System.Arg>): System => {
	let arch: System.Arch | undefined = undefined;
	let os: System.Os | undefined = undefined;
	args.forEach((arg) => {
		if (System.is(arg)) {
			arch = System.arch(arg);
			os = System.os(arg);
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
	return `${arch}-${os}` as System;
};

export type System =
	| "aarch64-darwin"
	| "aarch64-linux"
	| "js-js"
	| "x86_64-darwin"
	| "x86_64-linux";

export declare namespace System {
	let new_: (...args: Array<System.Arg>) => System;
	export { new_ as new };
}

export namespace System {
	export type Arg = System | ArgObject;

	export type ArgObject = {
		arch?: Arch;
		os?: Os;
	};

	export type Arch = "aarch64" | "js" | "x86_64";

	export type Os = "darwin" | "js" | "linux";
	export let new_ = (...args: Array<System.Arg>): System => {
		return system(...args);
	};
	System.new = new_;

	export let is = (value: unknown): value is System => {
		return (
			value === "aarch64-darwin" ||
			value === "aarch64-linux" ||
			value === "js-js" ||
			value === "x86_64-darwin" ||
			value === "x86_64-linux"
		);
	};

	export let expect = (value: unknown): System => {
		assert_(System.is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is System => {
		assert_(System.is(value));
	};

	export let arch = (system: System): Arch => {
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

	export let os = (system: System): Os => {
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
