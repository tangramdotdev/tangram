import bun from "bun" with { path: "../packages/packages/bun" };
import foundationdb from "foundationdb" with {
	path: "../packages/packages/foundationdb",
};
import { libclang } from "llvm" with { path: "../packages/packages/llvm" };
import { cargo } from "rust" with { path: "../packages/packages/rust" };
import xz from "xz" with { path: "../packages/packages/xz" };
import zlib from "zlib" with { path: "../packages/packages/zlib" };
import * as std from "std" with { path: "../packages/packages/std" };
import { $ } from "std" with { path: "../packages/packages/std" };

import source from "." with { type: "directory" };

export type Arg = {
	build?: string;
	foundationdb?: boolean;
	host?: string;
	nats?: boolean;
	postgres?: boolean;
};

export const build = async (arg?: Arg) => {
	const {
		build: build_,
		foundationdb: useFoundationdb = false,
		host: host_,
		nats = false,
		postgres = false,
	} = arg ?? {};
	const host = host_ ?? (await std.triple.host());
	if (useFoundationdb) {
		tg.assert(
			std.triple.os(host) === "linux",
			"the foundationdb feature is only available for Linux hosts",
		);
	}
	const build = build_ ?? host;
	const cargoLock = await source.get("Cargo.lock").then(tg.File.expect);

	// Collect environment.
	const envs: Array<tg.Unresolved<std.env.Arg>> = [
		bunEnvArg(build),
		librustyv8({ lockfile: cargoLock, host }),
	];

	// Set up node_modules.
	let pre = tg`
			mkdir node_modules
			cp -R ${nodeModules(build)}/. node_modules
			export NODE_PATH=$PWD/node_modules
			export PATH=$PATH:$NODE_PATH/.bin
	`;

	// Configure features.
	const features = [];
	if (nats) {
		features.push("nats");
	}
	if (postgres) {
		features.push("postgres");
	}
	if (useFoundationdb) {
		features.push("foundationdb");
		const fdbArtifact = foundationdb({ build, host });
		envs.push(fdbArtifact, {
			LIBCLANG_PATH: tg`${libclang({ build, host })}/lib`,
			FDB_LIB_PATH: tg`${fdbArtifact}/lib`,
		});
		pre = tg`
			${pre}
			export LD_LIBRARY_PATH=$LIBRARY_PATH
			export CPATH=$CPATH:$(gcc -print-sysroot)/include
		`;
	}

	// Build tangram.
	const env = std.env.arg(...envs);
	let output = cargo.build({
		...(await std.triple.rotate({ build, host })),
		buildInTree: true,
		disableDefaultFeatures: true,
		env,
		features,
		pre,
		source,
		useCargoVendor: true,
	});

	// Add xz library path.
	const libraryPaths = [];
	const xzLibDir = xz({ build, host })
		.then((d) => d.get("lib"))
		.then(tg.Directory.expect);
	libraryPaths.push(xzLibDir);

	// If building with foundationdb, additionally add zlib.
	if (useFoundationdb) {
		const zlibLibDir = zlib({ build, host })
			.then((d) => d.get("lib"))
			.then(tg.Directory.expect);
		libraryPaths.push(zlibLibDir);
	}

	// Wrap and return.
	const unwrapped = output
		.then((dir) => dir.get("bin/tangram"))
		.then(tg.File.expect);
	const wrapped = std.wrap(unwrapped, { libraryPaths });
	return await tg.directory(output, {
		["bin/tangram"]: wrapped,
		["bin/tg"]: tg.symlink("tangram"),
	});
};

export default build;

export type CloudArg = {
	build?: string;
	host?: string;
};

export const cloud = async (arg?: CloudArg) => {
	const host = arg?.host ?? (await std.triple.host());
	const build_ = arg?.build ?? host;
	return await build({
		build: build_,
		host,
		foundationdb: true,
		nats: true,
		postgres: true,
	});
};

export const nodeModules = async (hostArg?: string) => {
	const host = hostArg ?? (await std.triple.host());
	const hostOs = std.triple.os(host);
	const packageJson = source.get("package.json").then(tg.File.expect);
	const bunLock = source.get("bun.lock").then(tg.File.expect);
	const compiler = source.get("packages/compiler").then(tg.Directory.expect);
	const runtime = source.get("packages/runtime").then(tg.Directory.expect);
	const vscode = source.get("packages/vscode").then(tg.Directory.expect);
	const env = std.env.arg(bunEnvArg(host));
	let output = await $`
		mkdir work
		cd work
		cp ${packageJson} package.json
		cp ${bunLock} bun.lock
		mkdir packages
		cp -R ${compiler} packages/compiler
		cp -R ${runtime} packages/runtime
		cp -R ${vscode} packages/vscode
		mkdir -p $OUTPUT
		bun install --frozen-lockfile
		cp -R node_modules/. $OUTPUT
		`
		.checksum("sha256:any")
		.network(true)
		.env(env)
		.then(tg.Directory.expect);

	// On Linux, we need to wrap the biome executable.
	if (hostOs === "linux") {
		const hostArch = std.triple.arch(host);
		const pathArch = hostArch === "aarch64" ? "arm64" : "x64";
		const path = `@biomejs/cli-linux-${pathArch}/biome`;
		const unwrapped = await output.get(path).then(tg.File.expect);
		const wrapped = await std.wrap(unwrapped);
		output = await tg.directory(output, {
			[`${path}`]: wrapped,
		});
	}

	return output;
};

const bunEnvArg = async (hostArg?: string) => {
	const host = hostArg ?? (await std.triple.host());
	const hostOs = std.triple.os(host);
	const bunArtifact = await bun({ host });
	if (hostOs === "linux") {
		return std.env.arg(
			bunArtifact,
			tg.directory({ ["bin/node"]: tg.symlink(tg`${bunArtifact}/bin/bun`) }),
		);
	} else {
		return bunArtifact;
	}
};

export type LibRustyV8Arg = {
	host?: string;
	lockfile: tg.File;
	bindings?: boolean;
};

export const librustyv8 = async (arg: LibRustyV8Arg) => {
	const { host: hostArg, lockfile, bindings = false } = arg;
	const host = hostArg ?? (await std.triple.host());
	let os;
	if (std.triple.os(host) === "darwin") {
		os = "apple-darwin";
	} else if (std.triple.os(host) === "linux") {
		os = "unknown-linux-gnu";
	} else {
		throw new Error(`unsupported host ${host}`);
	}
	const checksum = "sha256:any";
	const file = `librusty_v8_release_${std.triple.arch(host)}-${os}.a.gz`;
	const version = await getRustyV8Version(lockfile);
	const lib = await std
		.download({
			checksum,
			mode: "decompress",
			url: `https://github.com/denoland/rusty_v8/releases/download/v${version}/${file}`,
		})
		.then(tg.File.expect);

	const result: Record<string, tg.File> = {
		RUSTY_V8_ARCHIVE: lib,
	};

	if (bindings) {
		const bindingFile = `src_binding_release_${std.triple.arch(host)}-${os}.rs`;
		const binding = await std
			.download({
				checksum,
				url: `https://github.com/denoland/rusty_v8/releases/download/v${version}/${bindingFile}`,
			})
			.then(tg.File.expect);
		result.RUSTY_V8_SRC_BINDING_PATH = binding;
	}

	return result;
};

const getRustyV8Version = async (lockfile: tg.File) => {
	const v8 = await lockfile
		.text()
		.then((t) => tg.encoding.toml.decode(t))
		.then((toml) =>
			(toml as CargoLock).package.find((pkg) => pkg.name === "v8"),
		);
	if (v8 === undefined) {
		throw new Error("Could not find v8 dependency in lockfile");
	}
	return v8.version;
};

type CargoLock = {
	package: Array<{ name: string; version: string }>;
};

export const test = async () => {
	const output = await $`tg --help > $OUTPUT`
		.env(build())
		.then(tg.File.expect)
		.then((f) => f.text());
	tg.assert(output.includes("Usage:"));
};

export const testCloud = async () => {
	const output = await $`tg --help > $OUTPUT`
		.env(cloud())
		.then(tg.File.expect)
		.then((f) => f.text());
	tg.assert(output.includes("Usage:"));
};

export const cross = async () =>
	build({
		build: "aarch64-unknown-linux-gnu",
		host: "x86_64-unknown-linux-gnu",
	});
