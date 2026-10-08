import cargoNextest from "cargo-nextest" with {
	source: "../packages/packages/cargo-nextest.tg.ts",
};
import { cargo, self as rust } from "rust" with {
	source: "../packages/packages/rust",
};
import * as std from "std" with { source: "../packages/packages/std" };

import source from "." with { type: "directory" };

import { toolchain } from "./tangram.ts";

export type Arg = cargo.Arg;

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
	const git = gitDependencies(source_, build);

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
		pre: tg.Template.join(
			"\n",
			tg`cp -R ${git} "$CARGO_HOME/git"
			chmod -R u+w "$CARGO_HOME/git"
			cd "$TGRUSTC_SOURCE_DIR"`,
			pre,
			pre_ ?? null,
		),
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

const gitDependencies = async (source: tg.Directory, host: string) => {
	// Cargo vendor cannot represent the two Ruff sources with identical package names and versions.
	const manifests = await tg.build(cargo.extractCargoManifests, source);
	const certFile = tg`${std.caCertificates()}/cacert.pem`;
	const dependencies = await std.build`
		export CARGO_HOME="${tg.output}"
		mkdir -p "$CARGO_HOME"
		cargo fetch --locked --manifest-path ${manifests}/Cargo.toml
	`
		.checksum("sha256:any")
		.named("fetch-rust-dependencies")
		.network(true)
		.env(std.sdk({ host }), rust({ host }), {
			CARGO_HTTP_CAINFO: certFile,
			CARGO_REGISTRIES_CRATES_IO_PROTOCOL: "sparse",
			SSL_CERT_FILE: certFile,
		})
		.then(tg.Directory.expect);
	return dependencies.get("git").then(tg.Directory.expect);
};
