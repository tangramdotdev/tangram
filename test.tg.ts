import bubblewrap from "bubblewrap" with {
	source: "../packages/packages/bubblewrap.tg.ts",
};
import bun from "bun" with { source: "../packages/packages/bun.tg.ts" };
import cargoNextest from "cargo-nextest" with {
	source: "../packages/packages/cargo-nextest.tg.ts",
};
import curl from "curl" with { source: "../packages/packages/curl.tg.ts" };
import delta from "delta" with { source: "../packages/packages/delta.tg.ts" };
import libfuse from "libfuse" with {
	source: "../packages/packages/libfuse.tg.ts",
};
import nodejs from "nodejs" with {
	source: "../packages/packages/nodejs.tg.ts",
};
import nushell from "nushell" with {
	source: "../packages/packages/nushell.tg.ts",
};
import passt from "passt" with {
	source: "../packages/packages/passt.tg.ts",
};
import procps from "procps" with {
	source: "../packages/packages/procps.tg.ts",
};
import python from "python" with { source: "../packages/packages/python" };
import { cargo } from "rust" with { source: "../packages/packages/rust" };
import * as std from "std" with { source: "../packages/packages/std" };
import utilLinux from "util-linux" with {
	source: "../packages/packages/util-linux.tg.ts",
};

import source from "." with { type: "directory" };

import { build, nodeModules, standalone } from "./tangram.ts";

export type E2eArg = {
	/** Regular expressions that select tests by their paths relative to packages/tests/tests. */
	filters?: Array<string>;

	/** Disable network access and skip the tests that require it. */
	offline?: boolean;
};

type UvLock = {
	package: Array<{
		dependencies?: Array<{ name: string }>;
		name: string;
		wheels?: Array<{ hash: string; url: string }>;
	}>;
};

/** Run each e2e test in its own sandbox. */
export const testE2e = async (arg: E2eArg = {}) => {
	const { filters = [], offline = false } = arg;
	const host = std.triple.host();

	// Build Tangram, the test runner, and the test supervisor without Tangram wrappers, because a test server runs Tangram in its sandboxes, which do not contain the artifacts that a wrapper loads.
	const tangram = await standalone({
		features: ["fjall", "quickjs", "rocksdb", "turso"],
		packages: [
			"tangram_cli",
			"tangram_tests_runner",
			"tangram_tests_supervisor",
		],
	});

	// Get the dynamic loader that the standalone executables request, and std's loader and libraries to install for them.
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
			source,
			useCargoVendor: true,
		})
		.then((output) => output.get("bin"))
		.then(tg.Directory.expect);

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

	// Run each test in its own sandbox.
	const env = std.env.arg(
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
		utilLinux(),
		{
			PYTHONPATH: pythonClient,
			SSL_CERT_FILE: tg`${std.caCertificates()}/cacert.pem`,
		},
	);
	const results = await Promise.allSettled(
		selected.map((test) => {
			const directory = test.includes("/")
				? test.slice(0, test.lastIndexOf("/"))
				: "";
			const filter = `/packages/tests/tests/${test.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`;
			return std.build`
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

				# Run the test in a user and mount namespace. The runner finds the test in the source that it was built from, so mount the repository with the clients over the source, and mount the writable copy over the test's directory, where the test writes its snapshots. The namespace also lets fusermount3 mount the test server's VFS.
				export PATH="${tangram}/bin:$PATH"
				unshare --user --map-current-user --mount --keep-caps \\
					sh -c 'mount --bind "$1" "$2" && mount --bind "$3" "$4" && shift 4 && exec tangram_tests_runner "$@"' \\
					sh ${repository} ${source} "$PWD/copy" ${source}/packages/tests/tests/${directory} \\
					run --jobs 1 --no-progress-details ${offline ? "--offline" : ""} -- '${filter}'
			`
				.named(test)
				.network(!offline)
				.checksum(offline ? null : "sha256:any")
				.env(env);
		}),
	);

	// Report the failed tests.
	const failures = selected.filter(
		(_, index) => results[index]?.status === "rejected",
	);
	if (failures.length > 0) {
		throw new Error(
			`${failures.length} of ${selected.length} tests failed:\n${failures.join("\n")}`,
		);
	}
};

/** Run the Rust tests in a sandbox. */
export const testRust = async () => {
	// Run nextest as `bun run test:rust` does. Its setup script builds the test runner with a nested Cargo invocation, so give that invocation this build's target directory and generated configuration.
	await build({
		env: std.env.arg(cargoNextest(), nushell(), { CARGO_NET_OFFLINE: "true" }),
		foundationdb: true,
		pre: `
			export CARGO_TARGET_DIR="$TARGET_DIR"
			cp "$cargo_config_path" "$CARGO_HOME/config.toml"
		`,
		profile: "dev",
		profileFlag: "--cargo-profile",
		subcommand: "nextest run --all-features --no-fail-fast --workspace",
	});
};
