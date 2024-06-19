import * as std from "tg:std" with { path: "../../../packages/packages/std" };
import { $ } from "tg:std" with { path: "../../../packages/packages/std" };
import tangram from "tg:tangram" with { path: "../.." };

export default tg.target(async () => {
	const previous = (await import("./snapshots")).default;
	const current = snapshots();
	const diff = await $`
		diff -Nqr -x '\.tangram' ${previous} ${current} | tee $OUTPUT
	`.then(tg.File.expect);
	return (await diff.text()) === "";
});

export const hello = tg.target(async () => {
	return 4;
});

/// Create the snapshot directory.
export const snapshots = tg.target(() =>
	tg.directory({
		artifacts: testArtifacts(),
		builds: testBuilds(),
		cli: testCli(),
		logs: testLogs(),
		mutations: testMutations(),
		packages: testPackages(),
	}),
);

/// Given a source directory, build the default target.
export const test = tg.target(async (source: tg.Directory) => {
	return await $`
		# Copy the source into the current working directory and set permissions.
		cp -R ${source}/. .
		chmod -R +w .

		# Run the default build, and check out the result to OUTPUT.
		tg build --no-tui -p $PWD -c $OUTPUT
	`
		.env(env())
		.then(tg.Directory.expect);
});

/// Construct a test environment.
export const env = tg.target(
	async (): Promise<tg.Directory> => {
		const config = tg.file(`
	{
		"advanced": {
			"error_trace_options": {
				"internal": true
			},
			"duplicate_build_logs_to_stderr": true
		},
		"path": "/tmp/.tangram",
		"remotes": null,
		"tracing": {
			"filter": "tangram_server=debug"
		},
		"vfs": null
	}
	`);
		const unwrappedTg = await tangram()
				.then((dir) => dir.get("bin/tg"))
				.then(tg.File.expect);
		return tg.directory({
			"bin/tg": std.wrap(unwrappedTg, {
				args: [tg`--config=${config}`, "--mode=server"],
			}),
		});
	},
);

// Artifact creation.
export const testArtifacts = tg.target(() => test(artifactsSource()));
export const artifactsSource = tg.target(async () =>
	tg.directory({
		"tangram.ts": (
			await import("./src/artifacts.ts", { with: { type: "file" } })
		).default,
	}),
);

// Builds.
export const testBuilds = tg.target(() => test(sandboxSource()));
export const sandboxSource = tg.target(async () =>
	tg.directory({
		"tangram.ts": (await import("./src/builds.ts", { with: { type: "file" } }))
			.default,
	}),
);

// CLI tests.
import testCli_ from "./cli.ts";
export const testCli = tg.target(() => testCli_());

import testLogs_ from "./logs.ts";
export const testLogs = tg.target(() => testLogs_());

// Mutation evaluation.
export const testMutations = tg.target(() => test(mutationsSource()));
export const mutationsSource = tg.target(async () =>
	tg.directory({
		"tangram.ts": (
			await import("./src/mutations.ts", { with: { type: "file" } })
		).default,
	}),
);

/// Package publishing and version solving tests.
import testPackages_ from "./packages.ts";
export const testPackages = tg.target(() => testPackages_());
