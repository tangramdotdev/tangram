import bun from "bun" with { source: "../packages/packages/bun.tg.ts" };
import dash from "dash" with { source: "../packages/packages/dash.tg.ts" };
import foundationdb from "foundationdb" with {
	source: "../packages/packages/foundationdb.tg.ts",
};
import libcapNg from "libcap-ng" with {
	source: "../packages/packages/libcap-ng.tg.ts",
};
import libseccomp from "libseccomp" with {
	source: "../packages/packages/libseccomp.tg.ts",
};
import { libclang } from "llvm" with { source: "../packages/packages/llvm" };
import openssl from "openssl" with {
	source: "../packages/packages/openssl.tg.ts",
};
import { cargo } from "rust" with { source: "../packages/packages/rust" };
import xz from "xz" with { source: "../packages/packages/xz.tg.ts" };
import zlib from "zlib-ng" with {
	source: "../packages/packages/zlib-ng.tg.ts",
};
import * as std from "std" with { source: "../packages/packages/std" };
import { $ } from "std" with { source: "../packages/packages/std" };

import source from "." with { type: "directory" };

export type Arg = cargo.Arg & {
	foundationdb?: boolean;
	nats?: boolean;
	postgres?: boolean;
	scylla?: boolean;
};

// Developer workflow targets.

export const check = async () => {
	await tg.run`cargo clippy --all-features --all-targets --workspace`;
	await tg.run`cd packages/clients/js && bunx tsgo && bunx oxlint src`;
	await tg.run`cd packages/js && bunx tsgo && bunx oxlint ./src/main.ts`;
	await tg.run`cd packages/typescript && bunx tsgo && bunx oxlint src`;
	await tg.run`cd packages/vscode && bunx tsgo && bunx oxlint extension.ts`;
};

export const format = async () => {
	await tg.run`cargo fmt --all`;
	await tg.run`bunx oxfmt --write packages/clients/js/src packages/js/src/tangram.d.ts packages/js/src/main.ts packages/typescript/src packages/vscode/extension.ts`;
};

export const run = async (...args: tg.Args<Arg>) => {
	const merged = await mergeArgs(args);
	const {
		disableDefaultFeatures = false,
		env: env_,
		host: host_,
		proxy = true,
		source: source_ = source,
	} = merged;
	const host = host_ ?? std.triple.host();
	const features = featureList(merged);

	// Build scripts run on the host in hybrid mode, sharing a target directory with bare cargo, so this omits every variable they watch with `rerun-if-env-changed` that bare cargo does not set: NODE_PATH, and the V8 archive from `librustyv8`, which only the sandboxed `build` export needs.
	const env = await std.env.arg(env_ ?? null, bunEnvArg(host));

	const output = cargo.run({
		disableDefaultFeatures,
		env,
		features,
		host,
		proxy,
		source: source_,
	});

	return output;
};

export default run;

// Build targets.

export const build = async (...args: tg.Args<Arg>) => {
	const merged = await mergeArgs(args);
	const {
		build: build_,
		captureStderr = false,
		disableDefaultFeatures = false,
		env: env_,
		foundationdb: useFoundationdb = false,
		host: host_,
		proxy = false,
		sdk,
		source: source_ = source,
	} = merged;
	const host = host_ ?? std.triple.host();
	const build = build_ ?? host;
	const features = featureList(merged);
	const cargoLock = await source_.get("Cargo.lock").then(tg.File.expect);

	// Collect environment.
	const envs: tg.Args<std.env.Arg> = [
		bunEnvArg(build),
		librustyv8(cargoLock, build, host),
		// `openssl-sys` locates openssl with pkg-config on behalf of the `native-tls` that `oauth2` pulls into `tangram_server`.
		openssl({ build, host }),
		sandboxRootfs(host),
	];

	// On Linux `tangram_vfs` links virtiofsd, which needs libcap-ng and libseccomp.
	if (std.triple.os(host) === "linux") {
		envs.push(libcapNg({ build, host }), libseccomp({ build, host }));
	}

	if (build !== host) {
		envs.push({
			[`CC_${host}`]: `${host}-cc`,
			[`CXX_${host}`]: `${host}-c++`,
		});
	}

	// Build node_modules and set NODE_PATH for esbuild and build scripts.
	const nodeModulesArtifact = nodeModules(build);
	envs.push({
		NODE_PATH: tg`${nodeModulesArtifact}/node_modules`,
		PATH: tg.Mutation.suffix(tg`${nodeModulesArtifact}/node_modules/.bin`, ":"),
	});

	// Configure foundationdb.
	let pre: tg.Unresolved<tg.Template.Arg> = null;
	if (useFoundationdb) {
		const fdbArtifact = foundationdb({ build, host });
		envs.push(fdbArtifact, {
			LIBCLANG_PATH: tg`${libclang({ build, host, ...std.args.optional("sdk", sdk) })}/lib`,
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
	const env = std.env.arg(...envs, env_ ?? null);
	const output = cargo.build({
		...(await std.triple.rotate({ build, host })),
		captureStderr,
		disableDefaultFeatures,
		env,
		features,
		pre,
		proxy,
		...std.args.optional("sdk", sdk),
		source: source_,
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
	const directory = await tg.directory(output, {
		["bin/tangram"]: wrapped,
		["bin/tg"]: tg.symlink("tangram"),
	});

	return directory;
};

export const cloud = async (...args: tg.Args<Arg>) => {
	const merged = await mergeArgs(args);
	const host = merged.host ?? std.triple.host();
	if (std.triple.os(host) !== "linux") {
		throw new Error(
			"the cloud configuration is only available for Linux hosts",
		);
	}
	const output = await build(
		{
			foundationdb: true,
			nats: true,
			postgres: true,
			scylla: true,
		},
		merged,
	);

	return output;
};

export const image = async (...args: tg.Args<Arg>) => {
	const dir = await build(...args);
	return await std.image(dir, {
		entrypoint: ["/bin/tangram"],
	});
};

export const release = async () => {
	const targets = [
		"aarch64-apple-darwin",
		"aarch64-unknown-linux-gnu",
		"x86_64-apple-darwin",
		"x86_64-unknown-linux-gnu",
	];
	const archives: Record<string, tg.File> = {};
	for (const target of targets) {
		// Build tangram for this target.
		const output = await build({ host: target });

		// Determine the archive name.
		const arch = std.triple.arch(target);
		let os: string;
		if (std.triple.os(target) === "darwin") {
			os = "darwin";
		} else {
			os = "linux";
		}
		const archiveName = `tangram_${arch}-${os}.tar.gz`;

		// Add a tgx wrapper script alongside the tangram binary.
		const tgx = tg.file("#!/bin/sh\ntg run -b $@", { executable: true });
		const releaseDir = await tg.directory(output, {
			["bin/tgx"]: tgx,
		});

		// Create a tar.gz archive.
		const archive = await $`
			mkdir -p $OUTPUT
			tar -czf $OUTPUT/${archiveName} -C ${releaseDir}/bin tangram tg tgx
		`
			.env(dash({ build: std.triple.host() }))
			.then(tg.Directory.expect)
			.then((d) => d.get(archiveName))
			.then(tg.File.expect);

		archives[archiveName] = archive;
	}
	return tg.directory(archives);
};

// Test targets.

export const test = async () => {
	await assertHelp(build());
};

export const testCloud = async () => {
	await assertHelp(cloud());
};

export const testProxy = async () => {
	await assertHelp(build({ profile: "dev", proxy: true }));
};

// Internal helpers.

const mergeArgs = async (args: tg.Args<Arg>) =>
	std.args.apply<Arg, Arg>({
		args,
		map: async (arg) => arg,
		reduce: {
			env: (a, b) => std.env.arg(a ?? null, b ?? null),
			features: "append",
			sdk: (a, b) => std.sdk.mergeArg(a, b),
		},
	});

const featureList = (arg: Arg) => {
	const features = [...(arg.features ?? [])];
	if (arg.nats) {
		features.push("nats");
	}
	if (arg.postgres) {
		features.push("postgres");
	} else {
		features.push("sqlite");
	}
	if (arg.scylla) {
		features.push("scylla");
	}
	if (arg.foundationdb) {
		features.push("foundationdb");
	} else {
		features.push("lmdb");
	}
	return features;
};

const assertHelp = async (env: tg.Unresolved<std.env.Arg>) => {
	const output = await $`tg --help > ${tg.output}`
		.env(env)
		.then(tg.File.expect)
		.then((f) => f.text);
	tg.assert(output.includes("Usage:"));
};

const nodeModules = async (hostArg?: string) => {
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

	const output = await std.build`
			cp -R ${workspaceSource}/. ${tg.output}
			chmod -R u+w ${tg.output}
			cd ${tg.output}
			bun install --frozen-lockfile --linker=hoisted || true
			mkdir -p packages/js/node_modules/@tangramdotdev
			ln -sf ../../../../clients/js packages/js/node_modules/@tangramdotdev/client
		`
		.checksum("sha256:any")
		.network(true)
		.env(bunEnvArg(host))
		.then(tg.Directory.expect);

	// Wrap the shebang scripts in node_modules/.bin as native executables with explicit interpreters so they do not need /usr/bin/env in the sandbox.
	const bunBin = await bun({ host })
		.then((d) => d.get("bin/bun"))
		.then(tg.File.expect);
	const dashBin = await dash({ host })
		.then((d) => d.get("bin/dash"))
		.then(tg.File.expect);
	const interpreters = { node: bunBin, sh: dashBin, bash: dashBin };
	const binDir = await output
		.get("node_modules/.bin")
		.then(tg.Directory.expect);
	const wrappedBin = await wrapShebangs(binDir, output, host, interpreters);
	// Provide `node` (bun) in .bin since many scripts use #!/usr/bin/env node.
	return tg.directory(output, {
		"node_modules/.bin": tg.directory(wrappedBin, { node: bunBin }),
	});
};

const wrapShebangs = async (
	binDir: tg.Directory,
	root: tg.Directory,
	host: string,
	interpreters: Record<string, tg.File>,
) => {
	let result = binDir;
	for await (const [name, artifact] of binDir) {
		// Resolve the entry to a file for metadata detection.
		let file: tg.File;
		if (artifact instanceof tg.Symlink) {
			try {
				const resolved = await root.get(`node_modules/.bin/${name}`);
				if (!(resolved instanceof tg.File)) continue;
				file = resolved;
			} catch {
				continue;
			}
		} else if (artifact instanceof tg.File) {
			file = artifact;
		} else {
			continue;
		}
		if (!(await file.executable)) continue;

		const metadata = await std.file.tryExecutableMetadata(file);
		if (!metadata || metadata.format !== "shebang") continue;

		let interpreter: tg.File | undefined;
		if (metadata.interpreter === "/usr/bin/env") {
			// For env shebangs, read the first line to get the command argument.
			const bytes = await file.read({ length: 128 });
			const text = tg.encoding.utf8.decode(bytes);
			const line = text.split("\n")[0] ?? "";
			const cmd = line.match(/^#!\s*\S+\s+(\S+)/)?.[1];
			if (cmd) {
				interpreter = interpreters[cmd];
			}
		} else {
			// Direct interpreter shebangs like #!/bin/sh or #!/bin/bash.
			const base = metadata.interpreter.split("/").pop();
			if (base) {
				interpreter = interpreters[base];
			}
		}
		if (!interpreter) continue;

		// Reference the script through a symlink into the root so the wrapper resolves it as a path rather than content, keeping the directory context that package.json subpath imports need.
		const executableRef = tg.symlink({
			artifact: root,
			path: `node_modules/.bin/${name}`,
		});
		const wrapped = await std.wrap({
			executable: executableRef,
			interpreter,
			host,
		});
		result = await tg.directory(result, { [name]: wrapped });
	}
	return result;
};

const bunEnvArg = async (hostArg?: string) => {
	const host = hostArg ?? std.triple.host();
	const bunArtifact = bun({ host });
	return std.env.arg(
		bunArtifact,
		tg.directory({ ["bin/node"]: tg.symlink(tg`${bunArtifact}/bin/bun`) }),
	);
};

const sandboxRootfs = async (hostArg?: string) => {
	const host = hostArg ?? std.triple.host();
	if (std.triple.os(host) !== "linux") {
		return {};
	}
	const arch = std.triple.arch(host);
	let archiveName: string;
	let checksum: tg.Checksum;
	if (arch === "aarch64") {
		archiveName = "sandbox_aarch64_linux.tar.zst";
		checksum =
			"sha256:7c7274baf07486c8314aa06e7bc7b0d69f2f39461ccbfa071f3b1a8b4cd26312";
	} else if (arch === "x86_64") {
		archiveName = "sandbox_x86_64_linux.tar.zst";
		checksum =
			"sha256:1a2ab509a1a2ab7e1f04bfd07cf5596ee27c8197b7f0e0ebe1ac037c07eb61bf";
	} else {
		throw new Error(`unsupported linux sandbox arch ${arch}`);
	}
	const url = `https://github.com/tangramdotdev/bootstrap/releases/download/v2026.07.29/${archiveName}`;
	const rootfs = await std.download
		.extractArchive({ checksum, url })
		.then(tg.Directory.expect);
	return { TANGRAM_SANDBOX_ROOTFS: rootfs };
};

const librustyv8 = async (lockfile: tg.File, ...hosts: Array<string>) => {
	const version = await getRustyV8Version(lockfile);
	const download = (name: string) =>
		std
			.download({
				checksum: "sha256:any",
				url: `https://github.com/denoland/rusty_v8/releases/download/v${version}/${name}`,
			})
			.then((b) => {
				tg.assert(b instanceof tg.Blob);
				return tg.file(b);
			});

	// A RUSTY_V8_ARCHIVE directory is searched for the library and src binding matching the build script's own TARGET, so one directory serves every triple.
	const entries: Record<string, Promise<tg.File>> = {};
	for (const host of new Set(hosts)) {
		let os: string;
		if (std.triple.os(host) === "darwin") {
			os = "apple-darwin";
		} else if (std.triple.os(host) === "linux") {
			os = "unknown-linux-gnu";
		} else {
			throw new Error(`unsupported host ${host}`);
		}
		const triple = `${std.triple.arch(host)}-${os}`;
		const library = `librusty_v8_release_${triple}.a.gz`;
		const binding = `src_binding_release_${triple}.rs`;
		entries[library] = download(library);
		entries[binding] = download(binding);
	}
	const archive = await tg.directory(entries);

	return { RUSTY_V8_ARCHIVE: archive };
};

const getRustyV8Version = async (lockfile: tg.File) => {
	const v8 = await lockfile.text
		.then((t) => tg.encoding.toml.decode(t))
		.then((toml) =>
			(toml as CargoLock).package.find((pkg) => pkg.name === "v8"),
		);
	if (v8 === undefined) {
		throw new Error("could not find the v8 package in the lockfile");
	}
	return v8.version;
};

type CargoLock = {
	package: Array<{ name: string; version: string }>;
};
