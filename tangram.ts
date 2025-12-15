import bun from "bun" with { local: "../packages/packages/bun.tg.ts" };
import foundationdb from "foundationdb" with {
	local: "../packages/packages/foundationdb.tg.ts",
};
import { libclang } from "llvm" with { local: "../packages/packages/llvm" };
import { cargo } from "rust" with { local: "../packages/packages/rust" };
import xz from "xz" with { local: "../packages/packages/xz.tg.ts" };
import zlib from "zlib" with { local: "../packages/packages/zlib.tg.ts" };
import * as std from "std" with { local: "../packages/packages/std" };
import { $ } from "std" with { local: "../packages/packages/std" };

import source from "." with { type: "directory" };

export type Arg = {
	build?: string;
	foundationdb?: boolean;
	host?: string;
	nats?: boolean;
	postgres?: boolean;
	sdk?: std.sdk.Arg;
	scylla?: boolean;
};

export const build = async (arg?: Arg) => {
	const {
		build: build_,
		foundationdb: useFoundationdb = false,
		host: host_,
		nats = false,
		postgres = false,
		sdk,
		scylla = false,
	} = arg ?? {};
	const host = host_ ?? std.triple.host();
	const build = build_ ?? host;
	const cargoLock = await source.get("Cargo.lock").then(tg.File.expect);

	// Collect environment.
	const envs: Array<tg.Unresolved<std.env.Arg>> = [
		bunEnvArg(build),
		librustyv8(cargoLock, host),
	];

	if (build !== host) {
		envs.push({
			[`CC_${host}`]: `${host}-cc`,
			[`CXX_${host}`]: `${host}-c++`,
		});
	}

	// Set up node_modules.
	const nodeModulesArtifact = nodeModules(build);
	let pre = tg`
			cp -R ${nodeModulesArtifact}/. "$SOURCE"
			export NODE_PATH="$SOURCE/node_modules"
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
	if (scylla) {
		features.push("scylla");
	}
	if (useFoundationdb) {
		if (std.triple.os(host) !== "linux") {
			throw new Error(
				"the foundationdb feature is only available for Linux hosts",
			);
		}
		features.push("foundationdb");
		const fdbArtifact = foundationdb({ build, host });
		envs.push(fdbArtifact, {
			LIBCLANG_PATH: tg`${libclang({ build, host, sdk })}/lib`,
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
		sdk,
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
	const wrapped = std.wrap(unwrapped, { host, libraryPaths });
	return await tg.directory(output, {
		["bin/tangram"]: wrapped,
		["bin/tg"]: tg.symlink("tangram"),
	});
};

export default build;

export type CloudArg = {
	build?: string;
	host?: string;
	sdk?: std.sdk.Arg;
};

export const cloud = async (arg?: CloudArg) => {
	const host_ = arg?.host ?? std.triple.host();
	const build_ = arg?.build ?? host_;
	const sdk = arg?.sdk;
	if (std.triple.os(host_) !== "linux") {
		throw new Error(
			"the cloud configuration is only available for Linux hosts",
		);
	}
	return await build({
		build: build_,
		host: host_,
		foundationdb: true,
		nats: true,
		postgres: true,
		sdk,
		scylla: true,
	});
};

export const nodeModules = async (hostArg?: string) => {
	const host = hostArg ?? std.triple.host();

	// Create subset of source relevant for bun install.
	const packageJson = source.get("package.json").then(tg.File.expect);
	const bunLock = source.get("bun.lock").then(tg.File.expect);
	const clientsJs = source.get("packages/clients/js").then(tg.Directory.expect);
	const js = source.get("packages/js").then(tg.Directory.expect);
	const typescript = source
		.get("packages/typescript")
		.then(tg.Directory.expect);
	const vscode = source.get("packages/vscode").then(tg.Directory.expect);

	const workspaceSource = tg.directory({
		"package.json": packageJson,
		"bun.lock": bunLock,
		packages: {
			clients: {
				js: clientsJs,
			},
			js,
			typescript,
			vscode,
		},
	});

	const output = await $`
			cp -R ${workspaceSource}/. ${tg.output}
			cd ${tg.output}
			bun install --frozen-lockfile || true
			mkdir -p packages/js/node_modules/@tangramdotdev
			ln -sf ../../../../clients/js packages/js/node_modules/@tangramdotdev/client
		`
		.checksum("sha256:any")
		.network(true)
		.env(bunEnvArg(host))
		.then(tg.Directory.expect);

	return output;
};

const bunEnvArg = async (hostArg?: string) => {
	const host = hostArg ?? std.triple.host();
	const bunArtifact = bun({ host });
	return std.env.arg(
		bunArtifact,
		tg.directory({ ["bin/node"]: tg.symlink(tg`${bunArtifact}/bin/bun`) }),
	);
};

export const librustyv8 = async (
	lockfile: tg.File,
	...hosts: Array<string>
) => {
	const hostList = hosts.length > 0 ? hosts : [std.triple.host()];
	const version = await getRustyV8Version(lockfile);

	const downloads = await Promise.all(
		hostList.map(async (host) => {
			let os;
			if (std.triple.os(host) === "darwin") {
				os = "apple-darwin";
			} else if (std.triple.os(host) === "linux") {
				os = "unknown-linux-gnu";
			} else {
				throw new Error(`unsupported host ${host}`);
			}
			const checksum = "sha256:any";
			const triple = `${std.triple.arch(host)}-${os}`;
			const file = `librusty_v8_release_${triple}.a.gz`;
			const lib = await std
				.download({
					checksum,
					url: `https://github.com/denoland/rusty_v8/releases/download/v${version}/${file}`,
				})
				.then((b) => {
					tg.assert(b instanceof tg.Blob);
					return tg.file(b);
				});
			const envVarSuffix = triple.replace(/-/g, "_");
			const key =
				hostList.length === 1
					? "RUSTY_V8_ARCHIVE"
					: `RUSTY_V8_ARCHIVE_${envVarSuffix}`;
			return { key, value: lib };
		}),
	);

	const result: Record<string, tg.File> = {};
	for (const { key, value } of downloads) {
		result[key] = value;
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
