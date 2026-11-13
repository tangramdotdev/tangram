import bubblewrap from "bubblewrap" with {
	source: "../packages/packages/bubblewrap.tg.ts",
};
import bun from "bun" with { source: "../packages/packages/bun.tg.ts" };
import cargoNextest from "cargo-nextest" with {
	source: "../packages/packages/cargo-nextest.tg.ts",
};
import curl from "curl" with { source: "../packages/packages/curl.tg.ts" };
import dash from "dash" with { source: "../packages/packages/dash.tg.ts" };
import delta from "delta" with { source: "../packages/packages/delta.tg.ts" };
import foundationdb from "foundationdb" with {
	source: "../packages/packages/foundationdb.tg.ts",
};
import libcapNg from "libcap-ng" with {
	source: "../packages/packages/libcap-ng.tg.ts",
};
import libfuse from "libfuse" with {
	source: "../packages/packages/libfuse.tg.ts",
};
import libseccomp from "libseccomp" with {
	source: "../packages/packages/libseccomp.tg.ts",
};
import { libclang } from "llvm" with { source: "../packages/packages/llvm" };
import nodejs from "nodejs" with {
	source: "../packages/packages/nodejs.tg.ts",
};
import nushell from "nushell" with {
	source: "../packages/packages/nushell.tg.ts",
};
import openssl from "openssl" with {
	source: "../packages/packages/openssl.tg.ts",
};
import passt from "passt" with {
	source: "../packages/packages/passt.tg.ts",
};
import procps from "procps" with {
	source: "../packages/packages/procps.tg.ts",
};
import python from "python" with { source: "../packages/packages/python" };
import { cargo, rustTriple } from "rust" with {
	source: "../packages/packages/rust",
};
import sqlite from "sqlite" with {
	source: "../packages/packages/sqlite.tg.ts",
};
import utilLinux from "util-linux" with {
	source: "../packages/packages/util-linux.tg.ts",
};
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

	// Hybrid builds share Cargo's target directory, so only sandboxed builds supply NODE_PATH and the V8 and Python artifacts watched by build scripts.
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
		pre: pre_,
		proxy = false,
		sdk,
		source: source_ = source,
		standalone = false,
		subcommand,
	} = merged;
	const host = host_ ?? std.triple.host();
	const build = build_ ?? host;
	const features = featureList(merged);
	const cargoLock = await source_.get("Cargo.lock").then(tg.File.expect);

	// Collect environment.
	const envs: tg.Args<std.env.Arg> = [
		bunEnvArg(build),
		librustyv8(cargoLock, build, host),
		libpython(source_, build, host),
		// `openssl-sys` locates openssl with pkg-config on behalf of the `native-tls` that `oauth2` pulls into `tangram_server`.
		openssl({ build, host }),
		sandboxRootfs(host),
	];

	// On Linux `tangram_vfs` links virtiofsd, which needs libcap-ng and libseccomp. Standalone builds link them statically.
	if (std.triple.os(host) === "linux") {
		const capNg = libcapNg({ build, host });
		const seccomp = libseccomp({ build, host });
		envs.push(capNg, seccomp);
		if (standalone) {
			envs.push({
				LIBCAPNG_LIB_PATH: tg`${capNg}/lib`,
				LIBCAPNG_LINK_TYPE: "static",
				LIBSECCOMP_LIB_PATH: tg`${seccomp}/lib`,
				LIBSECCOMP_LINK_TYPE: "static",
			});
		}
	}

	// Link OpenSSL and the libraries found with pkg-config statically into standalone builds.
	if (standalone) {
		envs.push({ OPENSSL_STATIC: "1", PKG_CONFIG_ALL_STATIC: "1" });
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

	// Configure bindgen, which generates the foundationdb and rocksdb bindings.
	let pre: tg.Unresolved<tg.Template.Arg> = null;
	if (useFoundationdb || features.includes("rocksdb")) {
		// Build libclang with the default linker, because the linker that the SDK argument names is for Tangram.
		const { linker: _linker, ...libclangSdk } = std.sdk.argObject(sdk) ?? {};
		const libclangArg =
			Object.keys(libclangSdk).length > 0 ? { sdk: libclangSdk } : {};
		envs.push({
			LIBCLANG_PATH: tg`${libclang({ build, host, ...libclangArg })}/lib`,
		});
		if (std.triple.os(host) === "linux") {
			// Give bindgen the C library's headers and libclang's resource directory, which holds the compiler's headers, such as stdbool.h.
			pre = tg`
				export CPATH=$CPATH:$(gcc -print-sysroot)/include
				export BINDGEN_EXTRA_CLANG_ARGS="-resource-dir $(echo "$LIBCLANG_PATH"/clang/*)"
			`;
		}
	}

	// Configure foundationdb.
	if (useFoundationdb) {
		const fdbArtifact = foundationdb({ build, host });
		envs.push(fdbArtifact, {
			FDB_LIB_PATH: tg`${fdbArtifact}/lib`,
		});
		if (std.triple.os(host) === "linux") {
			pre = tg`
				export LD_LIBRARY_PATH=$LIBRARY_PATH
				${pre}
			`;
		}
	}

	// Build tangram.
	const env = std.env.arg(...envs, env_ ?? null);
	const output = cargo.build({
		...merged,
		...(await std.triple.rotate({ build, host })),
		captureStderr,
		disableDefaultFeatures,
		env,
		features,
		pre: tg.Template.join("\n", pre, pre_ ?? null),
		proxy,
		...std.args.optional("sdk", sdk),
		source: source_,
		standalone,
		useCargoVendor: true,
	});

	// Return the output of other cargo subcommands, such as test runs, without wrapping.
	if (subcommand !== undefined) {
		return output;
	}

	// Return standalone builds unwrapped.
	if (standalone) {
		return tg.directory(output, { "bin/tg": tg.symlink("tangram") });
	}

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

/** Build a native executable that runs after checkout without a Tangram wrapper or artifact store. */
export const standalone = async (...args: tg.Args<Arg>) =>
	build(...args, { standalone: { glibcVersion: "2.39" } });

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

export type E2eArg = {
	/** Regular expressions that select tests by their paths relative to packages/tests/tests. */
	filters?: Array<string>;

	/** Disable network access and skip the tests that require it. */
	offline?: boolean;
};

export type E2eOutput = {
	/** The number of tests that passed. */
	passed: number;

	/** The skipped tests, by the reason that each test gave. */
	skipped: Record<string, Array<string>>;
};

/** Run each e2e test in its own sandbox, with only the artifacts that this module builds. Each test is a cached process, so run with --retry to run failed tests again. */
export const testE2e = async (arg: E2eArg = {}): Promise<E2eOutput> => {
	const { filters = [], offline = false } = arg;
	const host = std.triple.host();

	// Remove the e2e tests and their utilities from the source that Tangram is built from, because the runner reads them at run time from the repository that each test mounts over that source, so that changing a test does not rebuild Tangram.
	const buildSource = await tg.directory(source, {
		"packages/tests/tests": null,
		"packages/tests/util": null,
	});

	// Start building Tangram, the test runner, and the test supervisor without Tangram wrappers, because a test server runs Tangram in its sandboxes, which do not contain the artifacts that a wrapper loads.
	const tangramPromise = standalone({
		features: ["fjall", "quickjs", "rocksdb", "turso"],
		packages: [
			"tangram_cli",
			"tangram_tests_runner",
			"tangram_tests_supervisor",
		],
		source: buildSource,
	});

	// Configure PyO3 for the test interpreter instead of the embedded runtime that the repository configures, as packages/clients/python/build.py does. The Python extension does not link libpython, because the interpreter that imports it provides the symbols.
	const pythonArtifact = python({ enableOptimizations: false });
	const pyo3Config = await std.build`
		python3 -c 'import sys; print("implementation=CPython"); print("version=%d.%d" % sys.version_info[:2]); print("shared=true"); print("suppress_build_script_link_lines=true")' > ${tg.output}
	`
		.env(pythonArtifact)
		.then(tg.File.expect);

	// Build the native extensions of the JavaScript and Python clients.
	const extensions = await cargo
		.build({
			env: {
				PYO3_BUILD_EXTENSION_MODULE: "1",
				PYO3_CONFIG_FILE: pyo3Config,
			},
			packages: ["tangram_javascript_native", "tangram_python_native"],
			source: buildSource,
			useCargoVendor: true,
		})
		.then((output) => output.get("bin"))
		.then(tg.Directory.expect);

	// Get the dynamic loader that the standalone executables request, and std's loader and libraries to install for them.
	const tangram = await tangramPromise;
	const metadata = await tangram
		.get("bin/tangram")
		.then(tg.File.expect)
		.then(std.file.executableMetadata);
	tg.assert(
		metadata.format === "elf" && metadata.interpreter !== undefined,
		"expected a dynamically linked ELF executable",
	);
	const interpreter = metadata.interpreter;
	const { ldso, libDir } = await std.sdk.toolchainComponents({
		env: await std.env.arg(std.sdk()),
	});
	tg.assert(ldso !== undefined, "expected a dynamic loader");

	// Compile the JavaScript client, and add its native extension where the client loads it. The type definitions are hoisted to the root node_modules.
	const modules = await nodeModules();
	const arch = std.triple.arch(host) === "aarch64" ? "arm64" : "x64";
	const javascriptClient = await std.build`
		cd ${modules}/packages/clients/javascript
		tsc --outDir ${tg.output} --typeRoots ${modules}/node_modules/@types
		mkdir -p ${tg.output}/host
		cp ${extensions}/libtangram_javascript_native.so ${tg.output}/host/tangram_javascript_native.linux-${arch}.node
	`
		.env({
			PATH: tg.Mutation.prefix(tg`${modules}/node_modules/.bin`, ":"),
		})
		.then(tg.Directory.expect);

	// Add the Node.js packages and the compiled JavaScript client to the repository, where the Node.js tests import the client.
	const repository = await tg.directory(source, modules, {
		"packages/clients/javascript/dist": javascriptClient,
	});

	// Install the Python client and the dependencies that uv.lock pins for it, for the Python tests that import it.
	const lock = tg.encoding.toml.decode(
		await source
			.get("uv.lock")
			.then(tg.File.expect)
			.then((file) => file.text),
	) as UvLock;
	const packages = new Map(lock.package.map((entry) => [entry.name, entry]));
	const wheels: Array<Promise<tg.Directory>> = [];
	const install = (name: string, seen: Set<string>) => {
		const entry = packages.get(name);
		tg.assert(entry !== undefined, `uv.lock does not contain ${name}`);
		for (const dependency of entry.dependencies ?? []) {
			if (seen.has(dependency.name)) {
				continue;
			}
			seen.add(dependency.name);
			const wheel = packages
				.get(dependency.name)
				?.wheels?.find((wheel) => wheel.url.endsWith("-none-any.whl"));
			tg.assert(wheel !== undefined, `no wheel for ${dependency.name}`);
			wheels.push(
				std
					.download({ checksum: wheel.hash as tg.Checksum, url: wheel.url })
					.then(tg.Blob.expect)
					.then(tg.extract)
					.then(tg.Directory.expect),
			);
			install(dependency.name, seen);
		}
	};
	install("tangram", new Set());
	const pythonClient = await tg.directory(...(await Promise.all(wheels)), {
		tangram: tg.directory(
			source
				.get("packages/clients/python/src/tangram")
				.then(tg.Directory.expect),
			{
				"_native.so": extensions.get("libtangram_python_native.so"),
			},
		),
	});

	// Find the tests.
	const tests: Array<string> = [];
	const find = async (directory: tg.Directory, path: string) => {
		for await (const [name, artifact] of directory) {
			const subpath = path === "" ? name : `${path}/${name}`;
			if (artifact instanceof tg.Directory) {
				await find(artifact, subpath);
			} else if (name.endsWith(".nu")) {
				tests.push(subpath);
			}
		}
	};
	await find(
		await source.get("packages/tests/tests").then(tg.Directory.expect),
		"",
	);
	const selected = tests.filter(
		(test) =>
			filters.length === 0 ||
			filters.some((filter) => new RegExp(filter).test(test)),
	);

	// Create the environment for the tests.
	const env = await std.env.arg(
		std.sdk(),
		pythonArtifact,
		std.utils.attr.build(),
		bubblewrap(),
		bun(),
		curl(),
		delta(),
		libfuse(),
		nodejs(),
		nushell(),
		passt(),
		procps(),
		sqlite(),
		utilLinux(),
		{
			PYTHONPATH: pythonClient,
			SSL_CERT_FILE: tg`${std.caCertificates()}/cacert.pem`,
		},
	);

	// Run each test in its own sandbox, with a limited number at once, so that the sandboxes fit in the scheduler's queue, which holds 1024 by default. Each test writes whether it passed or why it was skipped.
	const limit = 512;
	const failed: Array<string> = [];
	let passed = 0;
	const skipped: Record<string, Array<string>> = {};
	const remaining = selected.values();
	const runTests = async () => {
		for (const test of remaining) {
			const directory = test.includes("/")
				? test.slice(0, test.lastIndexOf("/"))
				: "";
			const filter = `/packages/tests/tests/${test.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`;
			const output = await std.build`
				# Install std's loader at the path that the standalone executables request, and its libraries in /lib, where the loader searches.
				mkdir -p /lib "$(dirname ${interpreter})"
				ln -s ${ldso} ${interpreter}
				for library in ${libDir}/*; do
					[ -e "/lib/$(basename "$library")" ] || ln -s "$library" /lib/
				done

				# Copy the test's directory, where the test writes its snapshots, to a writable directory.
				cp -R ${source}/packages/tests/tests/${directory}/. copy
				chmod -R u+w copy

				# Remove the connection to this server and the environment values that refer to its store.
				unset TANGRAM_OUTPUT TANGRAM_TOKEN TANGRAM_URL
				for name in $(env | grep -o '^TANGRAM_ENV_[^=]*'); do
					unset "$name"
				done

				# Run the test in a user and mount namespace. The runner finds the test in the source that it was built from, so mount the repository with the clients over that source, and mount the writable copy over the test's directory, where the test writes its snapshots. The namespace also lets fusermount3 mount the test server's VFS.
				export PATH="${tangram}/bin:$PATH"
				status=0
				unshare --user --map-current-user --mount --keep-caps \\
					sh -c 'mount --bind "$1" "$2" && mount --bind "$3" "$4" && shift 4 && exec tangram_tests_runner "$@"' \\
					sh ${repository} ${buildSource} "$PWD/copy" ${buildSource}/packages/tests/tests/${directory} \\
					run --jobs 1 --no-progress-details ${offline ? "--offline" : ""} -- '${filter}' 2> log || status=$?
				cat log >&2
				if [ "$status" -ne 0 ]; then
					exit "$status"
				fi

				# Write the reason that the test was skipped, or that it passed.
				reason=$(sed -n 's/^.* skipped: //p' log)
				if [ -n "$reason" ]; then
					printf 'skipped: %s\\n' "$reason" > ${tg.output}
				else
					echo passed > ${tg.output}
				fi
			`
				.named(test)
				.network(!offline)
				.checksum(offline ? null : "sha256:any")
				.env(env)
				.then(tg.File.expect)
				.then((file) => file.text)
				.catch(() => undefined);
			if (output === undefined) {
				failed.push(test);
			} else if (output.startsWith("skipped: ")) {
				const reason = output.slice("skipped: ".length).trim();
				(skipped[reason] ??= []).push(test);
			} else {
				passed += 1;
			}
		}
	};
	await Promise.all(Array.from({ length: limit }, runTests));

	// Report the failed tests.
	const skippedCount = Object.values(skipped).flat().length;
	const summary = `${passed} passed, ${skippedCount} skipped, ${failed.length} failed`;
	if (failed.length > 0) {
		throw new Error(`${summary}:\n${failed.sort().join("\n")}`);
	}

	return { passed, skipped };
};

/** Run the Rust tests in a sandbox, with only the artifacts that this module builds. */
export const testRust = async () => {
	// Run nextest as `tg run .#testRust` does, with the standalone toolchain, whose executables are not Tangram wrappers, because the CLI tests' servers run Tangram in their sandboxes, which do not contain the artifacts that a wrapper loads. Nextest's setup script builds the test runner with a nested Cargo invocation, so give that invocation this build's target directory and generated configuration, and provide the /usr/bin/env that the setup script's interpreter line names.
	await build({
		env: std.env.arg(cargoNextest(), nushell(), zlib(), {
			CARGO_NET_OFFLINE: "true",
		}),
		foundationdb: true,
		pre: `
			export CARGO_TARGET_DIR="$SCRATCH_TARGET_DIR"
			cp "$cargo_config_path" "$CARGO_HOME/config.toml"
			if [ ! -e /usr/bin/env ]; then
				mkdir -p /usr/bin
				ln -s "$(command -v env)" /usr/bin/env
			fi
		`,
		profile: "dev",
		profileFlag: "--cargo-profile",
		sdk: { linker: "mold" },
		standalone: { glibcVersion: "2.39" },
		subcommand: "nextest run --all-features --no-fail-fast --workspace",
	});
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
	const clientsJavaScript = source
		.get("packages/clients/javascript")
		.then(tg.Directory.expect);
	const javascript = source
		.get("packages/javascript")
		.then(tg.Directory.expect);
	const vscode = source.get("packages/vscode").then(tg.Directory.expect);

	const workspaceSource = tg.directory({
		"package.json": packageJson,
		"bun.lock": bunLock,
		packages: {
			clients: {
				javascript: clientsJavaScript,
			},
			javascript,
			vscode,
		},
	});

	const output = await std.build`
			cp -R ${workspaceSource}/. ${tg.output}
			chmod -R u+w ${tg.output}
			cd ${tg.output}
			rm -f packages/*/tangram.ts packages/clients/*/tangram.ts
			bun install --frozen-lockfile --linker=hoisted || true
			mkdir -p packages/javascript/node_modules/@tangramdotdev
			ln -sf ../../../../clients/javascript packages/javascript/node_modules/@tangramdotdev/client
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
			"sha256:fa149313fbef0bb0287ceb9c3282ee2c31edd060e09a5c0cddf3d84361780710";
	} else if (arch === "x86_64") {
		archiveName = "sandbox_x86_64_linux.tar.zst";
		checksum =
			"sha256:cce4a90d4cc1893fc7a3f6b06f75661b865b8270b25b20555fea193069a98506";
	} else {
		throw new Error(`unsupported linux sandbox arch ${arch}`);
	}
	const url = `https://github.com/tangramdotdev/bootstrap/releases/download/v2026.09.16/${archiveName}`;
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

const libpython = async (source: tg.Directory, build: string, host: string) => {
	const manifest = await source
		.get("packages/python/packages/build/distributions.json")
		.then(tg.File.expect)
		.then((file) => file.text)
		.then((text) => JSON.parse(text) as PythonDistributions);
	const buildTriple = rustTriple(build);
	const hostTriple = rustTriple(host);
	const triples = new Set([buildTriple, hostTriple]);
	if (hostTriple.endsWith("-musl")) {
		triples.add(`${hostTriple}+static`);
	}

	// Supply both musl variants so the build script selects using Cargo's actual CRT flags.
	const entries: Record<string, Promise<tg.Directory>> = {};
	for (const triple of triples) {
		const artifact = manifest.targets[triple];
		if (artifact === undefined) {
			throw new Error(`unsupported Python target ${triple}`);
		}
		entries[triple] = std.download
			.extractArchive({
				checksum: `sha256:${artifact.sha256}`,
				url: artifact.url,
			})
			.then(tg.Directory.expect)
			.then((directory) => directory.get("python"))
			.then(tg.Directory.expect);
	}
	const distributions = await tg.directory(entries);
	const distribution = await distributions
		.get(buildTriple)
		.then(tg.Directory.expect);
	const metadata = await distribution
		.get("PYTHON.json")
		.then(tg.File.expect)
		.then((file) => file.text)
		.then((text) => JSON.parse(text) as { python_exe: string });

	// Preserve the interpreter's directory context so isolated Python can locate its standard library.
	const executable = await std.wrap({
		executable: tg.symlink({
			artifact: distribution,
			path: metadata.python_exe,
		}),
		host: build,
		libraryPaths: [{ path: tg`${distribution}/install/lib`, preserve: true }],
	});
	const hostDistribution = await tg.directory(distribution, {
		[metadata.python_exe]: executable,
	});
	const python = tg`${hostDistribution}/${metadata.python_exe}`;

	// Prepare the locked wheels separately, keeping Cargo's build scripts offline.
	const workspace = tg.directory({
		"uv.lock": source.get("uv.lock"),
	});
	const script = source.get("packages/python/packages/build/packages.py");
	const packages = await std.build`
		packages=$(${python} -I ${script} ${workspace} "$PWD/packages" ${python})
		cp -R "$packages" ${tg.output}
	`
		.host(build)
		.checksum("sha256:any")
		.network(true)
		.then(tg.Directory.expect);

	return {
		TANGRAM_PYTHON_DISTRIBUTION: distributions,
		TANGRAM_PYTHON_HOST_DISTRIBUTION: hostDistribution,
		TANGRAM_PYTHON_PACKAGES: packages,
	};
};

type PythonDistributions = {
	targets: Record<string, { sha256: string; url: string }>;
};

type CargoLock = {
	package: Array<{ name: string; version: string }>;
};

type UvLock = {
	package: Array<{
		dependencies?: Array<{ name: string }>;
		name: string;
		wheels?: Array<{ hash: string; url: string }>;
	}>;
};
