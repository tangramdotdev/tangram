import { assert as assert_, unreachable } from "./assert.ts";
import { getCurrentTarget } from "./target.ts";

export let triple = (arg: Triple.Arg): Triple => {
	let string = "";
	let arch = undefined;
	let os = undefined;
	let vendor = undefined;
	let environment = undefined;

	if (typeof arg === "string") {
		string = arg;
		let parts = arg.split("-");
		if (parts.length > 4) {
			throw new Error(`Invalid triple: ${arg}`);
		}

		arch = Triple.parseArch(parts[0] ?? "");

		if (parts.length === 4) {
			vendor = parts[1];
			os = Triple.parseOs(parts[2] ?? "");
			environment = Triple.parseEnvironment(parts[3] ?? "");
		} else {
			if (parts.length > 1) {
				let maybeOs = Triple.parseOs(parts[1] ?? "");
				if (maybeOs) {
					os = maybeOs;
				} else {
					vendor = parts[1];
				}
			}
			if (parts.length > 2) {
				if (os === undefined) {
					os = Triple.parseOs(parts[2] ?? "");
				} else {
					environment = Triple.parseEnvironment(parts[2] ?? "");
				}
			}
		}
	} else if (Triple.is(arg)) {
		return arg;
	} else {
		if (arg.arch) {
			string += `${arg.arch}`;
			arch = Triple.parseArch(arg.arch);
		}
		if (arg.vendor) {
			if (string.length > 0) {
				string += "-";
			}
			string += `${arg.vendor}`;
			vendor = arg.vendor;
		}
		if (arg.os) {
			if (string.length > 0) {
				string += "-";
			}
			string += `${arg.os}`;
			os = Triple.parseOs(arg.os);
		}
		if (arg.environment) {
			if (string.length > 0) {
				string += "-";
			}
			string += `${arg.environment}`;
			environment = Triple.parseEnvironment(arg.environment);
		}
	}
	return {
		string,
		arch,
		vendor,
		os,
		environment,
	};
};

export type Triple = {
	/** The original triple string. */
	string: string;
	/** The known architecture, if any. */
	arch?: Triple.Arch | undefined;
	/** The known environment, if any. */
	environment?: Triple.Environment | undefined;
	/** The known OS, if any. */
	os?: Triple.Os | undefined;
	/** The vendor, if any. */
	vendor?: string | undefined;
};

export declare namespace Triple {
	let new_: (...args: Array<Triple.Arg>) => Triple;
	export { new_ as new };
}

export namespace Triple {
	export type Arg = Triple | ArgObject | string;

	export type ArgObject = {
		arch?: Arch;
		environment?: string;
		os?: string;
		vendor?: string;
	};

	export namespace Arg {
		export let is = (value: unknown): value is Triple.Arg => {
			return (
				Triple.is(value) ||
				typeof value === "string" ||
				(typeof value === "object" &&
					value !== null &&
					("arch" in value ||
						"environment" in value ||
						"os" in value ||
						"vendor" in value))
			);
		};
	}

	export type HostArg =
		| Triple.Arg
		| {
				host?: Triple.Arg;
		  };

	/** Known architectures. */
	export type Arch = "aarch64" | "js" | "x86_64";

	export let arches: Set<Arch> = new Set(["aarch64", "js", "x86_64"]);

	export let parseArch = (value: string): Arch | undefined => {
		for (let arch of arches) {
			if (value.includes(arch)) {
				return arch;
			}
		}
		return undefined;
	};

	/** Known environments. */
	export type Environment = "gnu" | "musl";

	export let environments: Set<Environment> = new Set(["gnu", "musl"]);

	export let parseEnvironment = (value: string): Environment | undefined => {
		for (let env of environments) {
			if (value.includes(env)) {
				return env;
			}
		}
		return undefined;
	};

	/** Known operating systems. */
	export type Os = "darwin" | "linux";

	export let oss: Set<Os> = new Set(["darwin", "linux"]);

	export let parseOs = (value: string): Os | undefined => {
		for (let os of oss) {
			if (value.includes(os)) {
				return os;
			}
		}
		return undefined;
	};

	export let new_ = (arg: Triple.Arg): Triple => {
		return triple(arg);
	};
	Triple.new = new_;

	export let eq = (a: Triple, b: Triple): boolean => {
		return a.string === b.string;
	};

	export let is = (value: unknown): value is Triple => {
		return (
			typeof value === "object" &&
			value !== null &&
			"string" in value &&
			typeof value.string === "string"
		);
	};

	export let expect = (value: unknown): Triple => {
		assert_(Triple.is(value));
		return value;
	};

	export let assert = (value: unknown): asserts value is Triple => {
		assert_(Triple.is(value));
	};

	export let arch = (value: Triple): Arch => {
		let ret = value.arch;
		if (ret === undefined) {
			throw new Error("Expected arch");
		}
		return ret;
	};

	export let tryArch = (value: Triple): Arch | undefined => {
		return value.arch;
	};

	/** Construct a new Triple striping the vendor environment, and OS version. */
	export let archAndOs = (value: Triple): Triple => {
		let arch = Triple.arch(value);
		let os = Triple.os(value);
		return Triple.new({ arch, os });
	};

	export let environment = (value: Triple): Environment | undefined => {
		return value.environment;
	};

	export let environmentVersion = (value: Triple): string | undefined => {
		let parts = value.string.split("-");
		for (let part of parts) {
			for (let env of environments) {
				if (part.startsWith(env)) {
					return part.slice(env.length);
				}
			}
		}
		return undefined;
	};

	export let host = async (arg?: HostArg): Promise<Triple> => {
		if (arg === undefined) {
			let value = (await getCurrentTarget().env())["TANGRAM_HOST"] as string;
			return triple(value);
		} else if (Triple.is(arg)) {
			return arg;
		} else if (Triple.Arg.is(arg)) {
			return triple(arg);
		} else if ("host" in arg && Triple.Arg.is(arg.host)) {
			return triple(arg.host);
		} else {
			return unreachable();
		}
	};

	export let normalized = (value: Triple): string => {
		let arch = Triple.arch(value) ?? "unknown";
		let os = Triple.os(value) ?? "unknown";
		let vendor = Triple.vendor(value) ?? "unknown";
		let s = `${arch}-${vendor}-${os}`;
		let osVersion = Triple.osVersion(value);
		if (osVersion) {
			s += `${osVersion}`;
		}
		let env = Triple.environment(value);
		if (env) {
			s += `-${env}`;
		}
		let envVersion = Triple.environmentVersion(value);
		if (envVersion) {
			s += `${envVersion}`;
		}
		return s;
	};

	export let os = (value: Triple): Os => {
		let ret = value.os;
		if (ret === undefined) {
			throw new Error("Expected OS");
		}
		return ret;
	};

	export let tryOs = (value: Triple): Os | undefined => {
		return value.os;
	};

	export let osVersion = (value: Triple): string | undefined => {
		let parts = value.string.split("-");
		for (let part of parts) {
			for (let os of oss) {
				if (part.startsWith(os)) {
					return part.slice(os.length);
				}
			}
		}
		return undefined;
	};

	/** Take a package arg with optional build and host triples and produce the corresponding host and target triples for the SDK required to build it. */
	export let rotate = async (arg?: {
		build?: Triple.Arg;
		host?: Triple.Arg;
	}): Promise<{
		host: Triple;
		target: Triple;
	}> => {
		let host = await Triple.host(arg?.host);
		let build = arg?.build ? await Triple.host(arg.build) : host;
		return { host: build, target: host };
	};

	export let toString = (value: Triple): string => {
		return value.string;
	};

	export let vendor = (value: Triple): string | undefined => {
		return value.vendor;
	};
}
