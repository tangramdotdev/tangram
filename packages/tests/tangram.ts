import * as std from "tg:std" with { path: "../../../packages/packages/std" };
import { tangram } from "tg:tangram" with { path: "../../" };

export default tg.target(async () => {
	let previous = tg.include("snapshots");
	let current = snapshots();
	let diff = await tg.File.expect(
		await std.build(tg`
		diff -Nqr -x '\.tangram' ${previous} ${current} | tee $OUTPUT
	`),
	);
	return (await diff.text()) === "";
});

/// Create the snapshot directory.
export let snapshots = tg.target(() =>
	tg.directory({
		artifacts: testArtifacts(),
		builds: testBuilds(),
		cli: testCli(),
		mutations: testMutations(),
		packages: testPackages(),
	}),
);

/// Given a source directory, build the default target.
export let test = tg.target(async (source: tg.Directory) => {
	// Create the test script.
	let script = tg`
		# Copy the source into the current working directory and set permissions.
		cp -r ${source}/. .
		chmod -R +w .

		# Run the default build, and check out the result to OUTPUT.
		tg build --no-tui -p $PWD -c $OUTPUT
	`;

	// Run the build.
	let result = await std.build(script, { env: env() });
	return tg.Directory.expect(result);
});

/// The test environment.
export let env = tg.target(async (): Promise<tg.Directory> => {
	let config = tg.file(`
	{
		"advanced": {
			"error_trace_options": {
				"internal": true
			},
			"write_build_logs_to_stderr": true
		},
		"path": "/tmp/.tangram",
		"remotes": [],
		"tracing": {
			"filter": "tangram_server=debug"
		},
		"vfs": false
	}
	`);
	let unwrapped = tg.File.expect(await (await tangram()).get("bin/tg"));
	return tg.directory({
		"bin/tg": std.wrap(unwrapped, {
			args: [tg`--config=${config}`, "--mode=server"],
		}),
	});
});

// Artifact creation.
export let testArtifacts = tg.target(() => test(artifactsSource()));
export let artifactsSource = tg.target(() =>
	tg.directory({
		"tangram.ts": tg.include("./src/artifacts.ts"),
	}),
);

// Builds.
export let testBuilds = tg.target(() => test(sandboxSource()));
export let sandboxSource = tg.target(() =>
	tg.directory({
		"tangram.ts": tg.include("./src/builds.ts"),
	}),
);

// CLI tests.
import testCli_ from "./cli.ts";
export let testCli = tg.target(() => testCli_());

// Mutation evaluation.
export let testMutations = tg.target(() => test(mutationsSource()));
export let mutationsSource = tg.target(() =>
	tg.directory({
		"tangram.ts": tg.include("./src/mutations.ts"),
	}),
);

/// Package publishing and version solving tests.
import testPackages_ from "./packages.ts";
export let testPackages = tg.target(() => testPackages_());
