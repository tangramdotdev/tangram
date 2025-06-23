import bun from "bun" with { path: "../packages/packages/bun" };
import { cargo } from "rust" with { path: "../packages/packages/rust" };
import xz from "xz" with { path: "../packages/packages/xz" };
import * as std from "std" with { path: "../packages/packages/std" };
import { $ } from "std" with { path: "../packages/packages/std" };

import source from "." with { type: "directory" };

export const build = async () => {
	const host = await std.triple.host();
	const cargoLock = await source.get("Cargo.lock").then(tg.File.expect);
	const env = std.env.arg(bunEnvArg(host), librustyv8(cargoLock, host));
	const output = await cargo.build({
		buildInTree: true,
		disableDefaultFeatures: true,
		env,
		pre: tg`
			mkdir node_modules
			cp -R ${nodeModules(host)}/. node_modules
			export NODE_PATH=$PWD/node_modules
			export PATH=$PATH:$NODE_PATH/.bin
		`,
		source,
		useCargoVendor: true,
	});
	const xzLibDir = await xz({ host })
		.then((d) => d.get("lib"))
		.then(tg.Directory.expect);
	const unwrapped = await output.get("bin/tangram").then(tg.File.expect);
	const wrapped = await std.wrap(unwrapped, { libraryPaths: [xzLibDir] });
	return tg.directory({
		["bin/tangram"]: wrapped,
		["bin/tg"]: tg.symlink("tangram"),
	});
};

export default build;

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

export const librustyv8 = async (lockfile: tg.File, hostArg?: string) => {
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
	return {
		RUSTY_V8_ARCHIVE: lib,
	};
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
