import cargoNextest from "cargo-nextest" with {
	source: "../packages/packages/cargo-nextest.tg.ts",
};
import { cargo } from "rust" with { source: "../packages/packages/rust" };
import * as std from "std" with { source: "../packages/packages/std" };

import source from "." with { type: "directory" };

import { toolchainEnv } from "./tangram.ts";

export type Arg = cargo.Arg;

/** Run the Rust test suite with `cargo nextest` in a sandbox. The process takes no mounts and no network, so it is cacheable without a checksum. */
export const testRust = async (...args: tg.Args<Arg>) => {
	const merged = await std.args.apply<Arg, Arg>({
		args,
		map: async (arg) => arg,
		reduce: {
			env: (a, b) => std.env.arg(a ?? null, b ?? null),
			features: "append",
			sdk: (a, b) => std.sdk.mergeArg(a, b),
		},
	});
	const {
		env: env_,
		host: host_,
		source: source_ = source,
		subcommand = "nextest run --no-fail-fast --workspace",
		...rest
	} = merged;
	const host = host_ ?? std.triple.host();
	const cargoLock = await source_.get("Cargo.lock").then(tg.File.expect);

	// Collect the environment. `cargo nextest` resolves its subcommand by finding `cargo-nextest` on PATH.
	const env = std.env.arg(
		toolchainEnv({ build: host, cargoLock, host }),
		cargoNextest({ host }),
		env_ ?? null,
	);

	// Nextest claims `--profile` for a profile of its own, so the cargo profile goes under `--cargo-profile`. The tree must be writable because insta writes `.snap.new` beside the test.
	const output = await cargo.build(
		{
			buildInTree: true,
			env,
			host,
			processName: "test-rust",
			profileFlag: "--cargo-profile",
			source: source_,
			subcommand,
			useCargoVendor: true,
		},
		rest,
	);

	return output;
};

export default testRust;
