import bun from "bun" with { local: "../packages/packages/bun.tg.ts" };
import foundationdb from "foundationdb" with {
	local: "../packages/packages/foundationdb.tg.ts",
};
import { libclang } from "llvm" with { local: "../packages/packages/llvm" };
import { cargo } from "rust" with { local: "../packages/packages/rust" };
import xz from "xz" with { local: "../packages/packages/xz.tg.ts" };
import zlib from "zlib-ng" with { local: "../packages/packages/zlib-ng.tg.ts" };
import * as std from "std" with { local: "../packages/packages/std" };
import { $ } from "std" with { local: "../packages/packages/std" };

import source from "." with { type: "directory" };

export type Arg = cargo.Arg & {
	foundationdb?: boolean;
	nats?: boolean;
	postgres?: boolean;
	scylla?: boolean;
};

export const build = async (...args: std.Args<Arg>) => {
	const merged = await std.args.apply<Arg, Arg>({
		args,
		map: async (arg) => arg,
		reduce: {
			env: (a, b) => std.env.arg(a, b),
			features: "append",
			sdk: (a, b) => std.sdk.arg(a, b),
		},
	});
	const {
		build: build_,
		captureStderr = false,
		env: env_,
		foundationdb: useFoundationdb = false,
		host: host_,
		nats = false,
		postgres = false,
		proxy = false,
		sdk,
		scylla = false,
		source: source_ = source,
	} = merged;
	const host = host_ ?? std.triple.host();
	const build = build_ ?? host;
	const cargoLock = await source_.get("Cargo.lock").then(tg.File.expect);

	// Collect environment.
	const envs: std.Args<std.env.Arg> = [
		bunEnvArg(build),
		librustyv8(cargoLock, host),
	];

	if (build !== host) {
		envs.push({
			[`CC_${host}`]: `${host}-cc`,
			[`CXX_${host}`]: `${host}-c++`,
		});
	}

	// Merge the pre-built node_modules into the source.
	const nodeModulesArtifact = nodeModules(build);
	const sourceWithNodeModules = tg.directory(source_, nodeModulesArtifact);
	envs.push({
		NODE_PATH: tg`${nodeModulesArtifact}/node_modules`,
	});

	// Configure features.
	const features = [];
	if (nats) {
		features.push("nats");
	}
	if (postgres) {
		features.push("postgres");
	} else {
		features.push("sqlite");
	}
	if (scylla) {
		features.push("scylla");
	}
	if (!useFoundationdb) {
		features.push("lmdb");
	}
	let pre: tg.Unresolved<tg.Template.Arg | undefined>;
	if (useFoundationdb) {
		features.push("foundationdb");
		const fdbArtifact = foundationdb({ build, host });
		envs.push(fdbArtifact, {
			LIBCLANG_PATH: tg`${libclang({ build, host, sdk })}/lib`,
			FDB_LIB_PATH: tg`${fdbArtifact}/lib`,
		});
		if (std.triple.os(host) === "linux") {
			pre = tg`
				export LD_LIBRARY_PATH=$LIBRARY_PATH
				export CPATH=$CPATH:$(gcc -print-sysroot)/include
			`;
		}
	}

	// Build tangram.
	const env = std.env.arg(...envs, env_);
	let output = cargo.build({
		...(await std.triple.rotate({ build, host })),
		captureStderr,
		disableDefaultFeatures: true,
		env,
		features,
		pre,
		proxy,
		sdk,
		source: sourceWithNodeModules,
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

export const cloud = async (...args: std.Args<Arg>) => {
	const merged = await std.args.apply<Arg, Arg>({
		args,
		map: async (arg) => arg,
		reduce: {
			env: (a, b) => std.env.arg(a, b),
			sdk: (a, b) => std.sdk.arg(a, b),
		},
	});
	const host = merged.host ?? std.triple.host();
	if (std.triple.os(host) !== "linux") {
		throw new Error(
			"the cloud configuration is only available for Linux hosts",
		);
	}
	return await build(
		{
			foundationdb: true,
			nats: true,
			postgres: true,
			scylla: true,
		},
		merged,
	);
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
			chmod -R u+w ${tg.output}
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
	const v8 = await lockfile.text
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
	const output = await $`tg --help > ${tg.output}`
		.env(build())
		.then(tg.File.expect)
		.then((f) => f.text);
	tg.assert(output.includes("Usage:"));
};

export const testCloud = async () => {
	const output = await $`tg --help > ${tg.output}`
		.env(cloud())
		.then(tg.File.expect)
		.then((f) => f.text);
	tg.assert(output.includes("Usage:"));
};

export const testProxy = async () => {
	const output = await $`tg --help > ${tg.output}`
		.env(build({ proxy: true }))
		.then(tg.File.expect)
		.then((f) => f.text);
	tg.assert(output.includes("Usage:"));
};

/** Stats parsed from tgrustc tracing output in cargo-stderr.log. */
type ProxyStats = {
	kind: "proxy" | "runner";
	crate_name: string;
	cached: boolean;
	elapsed_ms: number;
	process_id: string;
	command_id: string;
};

/** Parse tgrustc stats from cargo-stderr.log in a build result. */
const parseStats = async (
	result: tg.Directory,
): Promise<Array<ProxyStats> | undefined> => {
	const stderrLog = await result
		.tryGet("cargo-stderr.log")
		.then((a) => (a instanceof tg.File ? a : undefined));
	if (!stderrLog) return undefined;

	const text = await stderrLog.text;
	const stats: Array<ProxyStats> = [];

	for (const line of text.split("\n")) {
		let kind: "proxy" | "runner" | undefined;
		if (line.includes("proxy_complete")) {
			kind = "proxy";
		} else if (line.includes("runner_complete")) {
			kind = "runner";
		} else {
			continue;
		}

		const crateMatch = /crate_name=(\S+)/.exec(line);
		const cachedMatch = /cached=(true|false)/.exec(line);
		const elapsedMatch = /elapsed_ms=(\d+)/.exec(line);
		const processMatch = /process_id=(\S+)/.exec(line);
		const commandMatch = /command_id=(\S+)/.exec(line);

		if (crateMatch?.[1] && cachedMatch?.[1]) {
			stats.push({
				kind,
				crate_name: crateMatch[1],
				cached: cachedMatch[1] === "true",
				elapsed_ms: elapsedMatch?.[1] ? parseInt(elapsedMatch[1], 10) : 0,
				process_id: processMatch?.[1] ?? "",
				command_id: commandMatch?.[1] ?? "",
			});
		}
	}

	return stats.length > 0 ? stats : undefined;
};

/** Test that the proxy correctly caches unchanged crates when only tangram_cli is modified. */
export const testProxyCacheHit = async () => {
	const tracingEnv = { TGRUSTC_TRACING: "tgrustc=info" };

	// First build: populate the cache.
	console.log("=== First build (populating cache) ===");
	await build({
		profile: "dev",
		proxy: true,
		captureStderr: true,
		env: tracingEnv,
	});

	// Modify only tangram_cli by adding a unique comment to main.rs.
	const mainRs = await source
		.get("packages/cli/src/main.rs")
		.then(tg.File.expect)
		.then((f: tg.File) => f.text);
	const modifiedSource = await tg.directory(source, {
		"packages/cli/src/main.rs": tg.file(
			`${mainRs}\n// Modified for cache test at ${Date.now()}\n`,
		),
	});

	// Second build: should cache hit for all crates except tangram.
	console.log("=== Second build (testing cache hits) ===");
	const secondResult = await build({
		profile: "dev",
		proxy: true,
		captureStderr: true,
		env: tracingEnv,
		source: modifiedSource,
	});
	const stats = await parseStats(secondResult);
	if (!stats) {
		throw new Error("Second build should have stats.");
	}
	const proxyStats = stats.filter((s) => s.kind === "proxy");
	const runnerStats = stats.filter((s) => s.kind === "runner");

	// The modified crate must be a cache miss.
	const tangramStats = proxyStats.find((s) => s.crate_name === "tangram");
	tg.assert(
		tangramStats !== undefined && tangramStats.cached === false,
		"The tangram crate should be a cache miss.",
	);

	// All other compilations must be cache hits.
	const proxyMisses = proxyStats.filter(
		(s) => !s.cached && s.crate_name !== "tangram",
	);
	tg.assert(
		proxyMisses.length === 0,
		`Expected all unchanged compilations to be cache hits, but these were misses: ${proxyMisses.map((s) => s.crate_name).join(", ")}`,
	);

	// All build script runners must be cache hits.
	const runnerMisses = runnerStats.filter((s) => !s.cached);
	tg.assert(
		runnerMisses.length === 0,
		`Expected all runners to be cache hits, but these were misses: ${runnerMisses.map((s) => s.crate_name).join(", ")}`,
	);

	return stats;
};
