import { beforeAll, expect, test } from "bun:test";
import * as fs from "node:fs/promises";

// Configuration of the server.
let url = "http+unix://%2Ftmp%2F.tangram%2Fsocket";

// Before running, build and check out the source code of the tests.
let sourcePath = "/tmp/testVersionSolvingSource";
beforeAll(async () => {
	await fs.rm(sourcePath, { recursive: true, force: true });
	let proc = Bun.spawn([
		"target/debug/tg",
		`--url=${url}`,
		"build",
		"-t",
		"testVersionSolvingSource",
		"-c",
		sourcePath,
	]);
	await proc.exited;
});

// Test harness. Publish dependencies then try and `tg tree` the dependant.
let runTest = async (name: string, dependencies: Array<string>) => {
	for (let dependency of dependencies) {
		console.log(`publishing ${sourcePath}/${name}/dependencies/${dependency}`);
		let proc = Bun.spawn([
			"target/debug/tg",
			`--url=${url}`,
			"publish",
			"-p",
			`${sourcePath}/${name}/dependencies/${dependency}`,
		]);
		await proc.exited;
	}
	console.log(`tg tree ${name}/dependant`);
	let proc = Bun.spawn(
		[
			"target/debug/tg",
			`--url=${url}`,
			"tree",
			`${sourcePath}/${name}/dependant`,
		],
		{
			stdout: "inherit",
		},
	);
	await proc.exited;
	expect(proc.exitCode).toBe(0);
};

// Test case: can we resolve a simple diamond dependency.
test("diamondDependencies", async () => {
	let dependencies = ["qux", "baz", "bar", "foo"];
	await runTest("diamondDependencies", dependencies);
});

// Test case: do we resolve the latest version.
test("latestVersion", async () => {
	let dependencies = [
		"latest@1.0.0",
		"latest@1.1.0",
		"latest@1.2.0",
		"latest@1.3.0",
	];
	await runTest("latestVersion", dependencies);
});

// Test case: can we resolve nested path dependencies in a registry dependency.
test("nestedPathDependencies", async () => {
	let dependencies = ["root"];
	await runTest("nestedPathDependencies", dependencies);
});

// Test case: does backtracking work as expected.
test("simpleBacktracking", async () => {
	let dependencies = ["baz@2.0.0", "baz@2.1.0", "bar@2.0.0", "bar@2.1.0"];
	runTest("simpleBacktracking", dependencies);
});
