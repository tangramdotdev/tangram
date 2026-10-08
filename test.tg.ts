import bun from "bun" with { source: "../packages/packages/bun.tg.ts" };
import cargoNextest from "cargo-nextest" with {
	source: "../packages/packages/cargo-nextest.tg.ts",
};
import fd from "fd" with { source: "../packages/packages/fd.tg.ts" };
import nushell from "nushell" with {
	source: "../packages/packages/nushell.tg.ts",
};
import procps from "procps" with {
	source: "../packages/packages/procps.tg.ts",
};
import { cargo } from "rust" with {
	source: "../packages/packages/rust",
};
import * as std from "std" with { source: "../packages/packages/std" };

import source from "." with { type: "directory" };

import { build as buildTangram, toolchain } from "./tangram.ts";

export type Arg = cargo.Arg;

export type CliArg = {
	/** Arguments for building the CLI binary. */
	build?: cargo.Arg;
	env?: std.env.Arg;
	/** Test name filters, as accepted by packages/cli/test.nu. */
	filters?: Array<string>;
	host?: string;
	jobs?: number;
	/** Disable network access and skip tests that download fixtures. */
	offline?: boolean;
	source?: tg.Directory;
	/** A prebuilt CLI installation, with bin/tangram. */
	tangram?: tg.Directory;
	timeout?: string;
};

/** Run the local-backend CLI suite in a sandbox. External Node and Python client tests are excluded. */
export const testCli = async (arg: CliArg = {}) => {
	const {
		build = {},
		env,
		filters = [],
		host = std.triple.host(),
		jobs = 2,
		offline = false,
		source: source_ = source,
		tangram: tangram_,
		timeout = "2min",
	} = arg;
	tg.assert(
		std.triple.os(host) === "linux",
		"testCli currently requires Linux",
	);
	tg.assert(
		Number.isInteger(jobs) && jobs > 0,
		"jobs must be a positive integer",
	);
	const tangram =
		tangram_ ??
		buildTangram({
			features: ["fjall", "quickjs", "rocksdb", "turso"],
			foundationdb: true,
			packages: ["tangram_cli"],
			parallelJobs: jobs,
			...build,
			host,
			source: source_,
		});
	const options = tg.file(JSON.stringify({ filters, jobs, offline, timeout }));
	const runner = tg.file`
		def main [options_path: path, tangram_path: path] {
			let options = open --raw $options_path | from json
			let offline = if $options.offline { ['--offline'] } else { [] }
			nu packages/cli/test.nu --no-cloud --no-clients --no-progress-details --preserve-failing-temps --tangram-path $tangram_path --jobs $options.jobs --timeout ($options.timeout | into duration) ...$offline ...$options.filters
		}
	`;
	const output = await std.build`
		mkdir -p ${tg.output}
		cp -R ${source_}/. work
		chmod -R u+w work
		cd work
		status=0
		nu ${runner} ${options} ${tangram}/bin/tangram > ${tg.output}/tests.log 2>&1 || status=$?
		cat ${tg.output}/tests.log
		exit "$status"
	`
		.named("test-cli")
		.network(!offline)
		.checksum(offline ? null : "sha256:any")
		.env(
			std.env.arg(
				std.sdk({ host }),
				nushell({ host }),
				fd({ host }),
				procps({ host }),
				bun({ host }),
				{
					SSL_CERT_FILE: tg`${std.caCertificates()}/cacert.pem`,
				},
				env ?? null,
			),
		)
		.then(tg.Directory.expect);
	return output;
};

/** Run the Rust test suite in a sandbox without mounts or network access by default. */
export const testRust = async (...args: tg.Args<Arg>) => {
	const {
		build,
		env: env_,
		host,
		pre: pre_,
		sdk,
		source: source_,
		subcommand = "nextest run --no-fail-fast --workspace",
		...rest
	} = await cargo.arg({ source }, ...args);
	const cargoLock = await source_.get("Cargo.lock").then(tg.File.expect);
	const { env, pre } = await toolchain({
		build,
		cargoLock,
		foundationdb: true,
		host,
		...std.args.optional("sdk", sdk),
		source: source_,
	});

	// Nextest uses --cargo-profile for Cargo's profile. Insta needs writable sources for pending snapshots.
	const output = await cargo.build(rest, {
		buildInTree: true,
		env: std.env.arg(
			env,
			cargoNextest({ host }),
			{ SSL_CERT_FILE: tg`${std.caCertificates()}/cacert.pem` },
			env_ ?? null,
		),
		host,
		pre: tg.Template.join("\n", pre, pre_ ?? null),
		processName: "test-rust",
		profileFlag: "--cargo-profile",
		...std.args.optional("sdk", sdk),
		source: source_,
		subcommand,
		useCargoVendor: false,
	});

	return output;
};

export default testRust;
