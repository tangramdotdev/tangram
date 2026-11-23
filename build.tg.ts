import bubblewrap from "bubblewrap" with {
	source: "../packages/packages/bubblewrap.tg.ts",
};
import bun from "bun" with { source: "../packages/packages/bun.tg.ts" };
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
import { context } from "./scripts/context.tg.ts";

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
		proxy = false,
		sdk,
		source: source_ = source,
		standalone = false,
	} = merged;
	const host = host_ ?? std.triple.host();
	const build = build_ ?? host;
	const features = featureList(merged);
	const cargoLock = await source_.get("Cargo.lock").then(tg.File.expect);

	// Compile the C libraries that a standalone Linux build links statically against the deployment libc's headers. Only the compiler flags apply, because the libraries' own executables link them dynamically.
	let cLibraryEnv: std.env.Arg | undefined;
	if (standalone && std.triple.os(host) === "linux") {
		const options = await std.sdk.standalone({
			...(typeof standalone === "object" ? standalone : {}),
			host: build,
			target: host,
		});
		const compileFlags = tg.Template.join(" ", ...options.compileArgs);
		cLibraryEnv = {
			CFLAGS: await tg.Mutation.suffix(compileFlags, " "),
			CXXFLAGS: await tg.Mutation.suffix(compileFlags, " "),
		};
	}
	const cLibraryArg = {
		build,
		host,
		...std.args.optional("env", cLibraryEnv),
	};

	// Collect environment.
	const envs: tg.Args<std.env.Arg> = [
		bunEnvArg(build),
		librustyv8(cargoLock, build, host),
		libpython(source_, build, host),
		// `openssl-sys` locates openssl with pkg-config on behalf of the `native-tls` that `oauth2` pulls into `tangram_server`.
		openssl(cLibraryArg),
		sandboxRootfs(host),
	];

	// On Linux `tangram_vfs` links virtiofsd, which needs libcap-ng and libseccomp. Standalone builds link them statically.
	if (std.triple.os(host) === "linux") {
		const capNg = libcapNg(cLibraryArg);
		const seccomp = libseccomp(cLibraryArg);
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

	// Compile C for the host with the cross compiler, and let the pkg-config crate run in a cross build, because every library that pkg-config finds here is built for the host.
	if (build !== host) {
		envs.push({
			[`CC_${host}`]: `${host}-cc`,
			[`CXX_${host}`]: `${host}-c++`,
			PKG_CONFIG_ALLOW_CROSS: "1",
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
		standalone,
		useCargoVendor: true,
	});

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

export const release = async (arg?: { commit?: string }) => {
	const targets = [
		"aarch64-apple-darwin",
		"aarch64-unknown-linux-gnu",
		"x86_64-apple-darwin",
		"x86_64-unknown-linux-gnu",
	];
	const machine = std.triple.host();
	const archives: Record<string, tg.File> = {};
	for (const target of targets) {
		// Skip the targets of other operating systems, which this machine cannot build.
		if (std.triple.os(target) !== std.triple.os(machine)) {
			continue;
		}

		// Spell a native host like the machine, because std compares triples as strings.
		const native =
			std.sdk.canonicalTriple(machine) === std.sdk.canonicalTriple(target);
		const host = native ? machine : target;

		// Build a standalone tangram for this target that reports the commit, linked against an old glibc on Linux.
		const standalone =
			std.triple.os(target) === "linux" ? { glibcVersion: "2.35" } : true;
		const env = arg?.commit
			? { TANGRAM_CLI_COMMIT_HASH: arg.commit }
			: undefined;
		const output = await build({
			build: machine,
			host,
			standalone,
			...std.args.optional("env", env),
		});

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
		const archive =
			await $`tar -czf ${tg.output} -C ${releaseDir}/bin tangram tg tgx`
				.env(dash({ build: machine }))
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

export type IntegrationArg = {
	/** Regular expressions that select tests by their paths relative to packages/tests/tests. */
	filters?: Array<string>;
};

/** Run the integration tests in cached sandboxes, with Tangram and the test runner built on the host in target/integration. Run it with `tg run ./build.tg.ts#testIntegration`. */
export const testIntegration = async (arg: IntegrationArg = {}) => {
	const { filters = [] } = arg;
	const host = std.triple.host();
	const { cwd: root } = await context();

	// Configure PyO3 for the test interpreter, as packages/clients/python/build.py does.
	const pythonArtifact = python({ enableOptimizations: false });
	const pyo3Config = await std.build`
		python3 -c 'import sys; print("implementation=CPython"); print("version=%d.%d" % sys.version_info[:2]); print("shared=true"); print("suppress_build_script_link_lines=true")' > ${tg.output}
	`
		.env(pythonArtifact)
		.then(tg.File.expect);

	// Build Tangram, the test runner, the test supervisor, and the client extensions on the host.
	const tangram = await buildIntegration({ host, pyo3Config, root });
	const extensions = await tangram.get("lib").then(tg.Directory.expect);

	// Get the dynamic loader that the executables request, and std's loader and libraries.
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

	// Compile the JavaScript client with its native extension.
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

	// Add the Node.js packages and the JavaScript client to the repository that the tests mount.
	const repository = await tg.directory(source, modules, {
		"packages/clients/javascript/dist": javascriptClient,
	});

	// Install the Python client and the dependencies that uv.lock pins for it.
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

	// Group the tests into batches of at most 16 tests from one directory.
	const directories = new Map<string, Array<string>>();
	for (const test of selected) {
		const directory = test.includes("/")
			? test.slice(0, test.lastIndexOf("/"))
			: "";
		const tests = directories.get(directory) ?? [];
		tests.push(test);
		directories.set(directory, tests);
	}
	const batchSize = 16;
	const batches: Array<{ directory: string; tests: Array<string> }> = [];
	for (const [directory, tests] of directories) {
		for (let start = 0; start < tests.length; start += batchSize) {
			batches.push({ directory, tests: tests.slice(start, start + batchSize) });
		}
	}

	// Run each batch in a sandbox, one test at a time, and read each test's result from the runner's output.
	const failed: Record<string, string> = {};
	let passed = 0;
	let skipped = 0;
	const runBatch = async (batch: {
		directory: string;
		tests: Array<string>;
	}) => {
		const { directory, tests } = batch;
		const filters = tests
			.map(
				(test) =>
					`'/packages/tests/tests/${test.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$'`,
			)
			.join(" ");
		const text = await std.build`
			# Install std's loader at the path that the executables request, and its libraries in /lib.
			mkdir -p /lib "$(dirname ${interpreter})"
			ln -s ${ldso} ${interpreter}
			ln -s ${libDir}/* /lib/ 2> /dev/null || true

			# Copy the directory to a writable directory for the tests' snapshots.
			cp -R ${source}/packages/tests/tests/${directory}/. copy
			chmod -R u+w copy

			# Remove the connection to this server.
			unset TANGRAM_OUTPUT TANGRAM_TOKEN TANGRAM_URL
			for name in $(env | grep -o '^TANGRAM_ENV_[^=]*'); do
				unset "$name"
			done

			# Mount the repository at the path that the runner was built from, with the writable copy over the directory, and run the tests.
			export PATH="${tangram}/bin:$PATH"
			mkdir -p '${root}'
			unshare --user --map-current-user --mount --keep-caps \\
				sh -c 'mount --bind "$1" "$2" && mount --bind "$3" "$4" && shift 4 && exec tangram_tests_runner "$@"' \\
				sh ${repository} '${root}' "$PWD/copy" '${root}/packages/tests/tests/${directory}' \\
				run --jobs 1 --no-progress-details -- ${filters} 2> log || true
			cat log >&2
			cp log ${tg.output}
		`
			.named(directory === "" ? "tests" : directory)
			.network(true)
			.checksum("sha256:any")
			.env(env)
			.then(tg.File.expect)
			.then((file) => file.text);
		const results = readResults(removeColors(text));
		for (const test of tests) {
			const result = results.get(test);
			if (result === undefined) {
				failed[test] = "the runner reported no result";
			} else if (result.kind === "failed") {
				failed[test] = result.message;
			} else if (result.kind === "skipped") {
				skipped += 1;
			} else {
				passed += 1;
			}
		}
	};
	await Promise.all(batches.map(runBatch));

	// Report the failed tests with their errors.
	const failedTests = Object.keys(failed).sort();
	const summary = `${passed} passed, ${skipped} skipped, ${failedTests.length} failed`;
	if (failedTests.length > 0) {
		const lines = failedTests.map((test) => `${test}: ${failed[test]}`);
		throw new Error(`${summary}:\n${lines.join("\n")}`);
	}

	return summary;
};

/** Run the Rust tests with nextest on the host in the repository's target directory, compiling the dependencies through the rustc proxy. Run it with `tg run -b ./build.tg.ts#testRust`, and pass nextest's arguments after `--`. */
export const testRust = async () => {
	const host = std.triple.host();
	const env = await std.env.arg(bunEnvArg(host));
	const pre =
		'set -- nextest run --all-features --no-fail-fast --workspace "$@"';
	// Link the proxied dependencies with mold, so that rustflags that select mold also work in the proxy's sandboxes.
	const sdk = { linker: "mold" as const };
	const command = await cargo.run({ env, host, pre, proxy: true, sdk, source });
	return command;
};

// Internal helpers.

/** Build Tangram, the test runner, the test supervisor, and the client extensions on the host, and output the executables in bin and the extensions in lib. */
const buildIntegration = async (arg: {
	host: string;
	pyo3Config: tg.File;
	root: string;
}) => {
	const { host, pyo3Config, root } = arg;
	const linker = `CARGO_TARGET_${rustTriple(host).toUpperCase().replaceAll("-", "_")}_LINKER`;

	// Link with std's SDK without Tangram wrappers, so that the executables run in the test sandboxes. Build in separate target directories, because the linker is in every crate's fingerprint.
	const pre = tg`
		export TANGRAM_LINKER_PASSTHROUGH=1
		export CC="$TGRUSTC_SANDBOX_SDK/bin/cc"
		export CXX="$TGRUSTC_SANDBOX_SDK/bin/c++"
		export ${linker}="$TGRUSTC_SANDBOX_SDK/bin/cc"
		CARGO_TARGET_DIR="$PWD/target/integration" cargo build --features fjall,quickjs,rocksdb,turso --package tangram_cli --package tangram_tests_runner --package tangram_tests_supervisor
		CARGO_TARGET_DIR="$PWD/target/integration-extensions" PYO3_BUILD_EXTENSION_MODULE=1 PYO3_CONFIG_FILE=${pyo3Config} cargo build --package tangram_javascript_native --package tangram_python_native
		mkdir -p "$TANGRAM_OUTPUT/bin" "$TANGRAM_OUTPUT/lib"
		cp target/integration/debug/tangram target/integration/debug/tangram_tests_runner target/integration/debug/tangram_tests_supervisor "$TANGRAM_OUTPUT/bin"
		ln -s tangram "$TANGRAM_OUTPUT/bin/tg"
		cp target/integration-extensions/debug/libtangram_javascript_native.so target/integration-extensions/debug/libtangram_python_native.so "$TANGRAM_OUTPUT/lib"
		exit
	`;

	// Link the proxied dependencies with mold, so that rustflags that select mold also work in the proxy's sandboxes.
	const sdk = { linker: "mold" as const };

	// Link libcap-ng, libseccomp, and OpenSSL statically, because the test sandboxes do not contain the host's libraries.
	const capNg = libcapNg({ build: host, host });
	const seccomp = libseccomp({ build: host, host });
	const env = await std.env.arg(bunEnvArg(host), {
		LIBCAPNG_LIB_PATH: tg`${capNg}/lib`,
		LIBCAPNG_LINK_TYPE: "static",
		LIBSECCOMP_LIB_PATH: tg`${seccomp}/lib`,
		LIBSECCOMP_LINK_TYPE: "static",
		OPENSSL_DIR: openssl({ build: host, host }),
		OPENSSL_STATIC: "1",
	});

	// Run the build on the host in the repository.
	const command = await cargo.run({ env, host, pre, sdk, source });
	const output = await tg.run(command).cwd(root).then(tg.Directory.expect);

	return output;
};

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

// Remove the colors that the runner writes around the symbol of each result.
const removeColors = (text: string): string =>
	text.replaceAll(new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, "g"), "");

type TestResult =
	| { kind: "failed"; message: string }
	| { kind: "passed" }
	| { kind: "skipped" };

// Read each test's first result from the runner's output: ✓ passed, ⊘ skipped, and ✗ failed, followed by the test's output.
const readResults = (text: string): Map<string, TestResult> => {
	const results = new Map<string, TestResult>();
	const lines = text.split("\n");
	const marker = /^([✓⊘✗]) (\S+\.nu) /;
	for (const [index, line] of lines.entries()) {
		const match = marker.exec(line);
		if (match === null) {
			continue;
		}
		const [, symbol, test] = match;
		if (test === undefined || results.has(test)) {
			continue;
		}
		if (symbol === "✓") {
			results.set(test, { kind: "passed" });
		} else if (symbol === "⊘") {
			results.set(test, { kind: "skipped" });
		} else {
			const rest = lines.slice(index + 1);
			const end = rest.findIndex((line) => marker.test(line));
			const block = end === -1 ? rest : rest.slice(0, end);
			const message =
				block
					.map((line) => line.trim())
					.find((line) => line.startsWith("x ")) ?? "the test failed";
			results.set(test, { kind: "failed", message });
		}
	}
	return results;
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
