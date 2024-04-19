import { beforeAll, expect, test } from "bun:test";
import * as fs from "node:fs/promises";

// Before running, build and check out the source code of the tests.
let configPath = "packages/tests/config.json";
let sourcePath = "/tmp/testVersionSolvingSource";
let tangramPath = "/tmp/.tangram";

beforeAll(async () => {
	console.log("cleaning...");
	await fs.rm(tangramPath, { recursive: true, force: true });
	await fs.rm(sourcePath, { recursive: true, force: true });

	console.log("building source...");
	let proc = Bun.spawn([
		"target/debug/tg",
		"--mode=server",
		`--config=${configPath}`,
		"build",
		"-t",
		"testVersionSolvingSource",
		"-c",
		sourcePath,
		"--no-tui",
	]);
	await proc.exited;
	expect(proc.exitCode).toBe(0);
});

// Test harness. Publish dependencies then try and `tg tree` the dependant.
let runTest = async (name: string, dependencies: Array<string>, expected) => {
	for (let dependency of dependencies) {
		console.log(`publishing ${sourcePath}/${name}/dependencies/${dependency}`);
		try {
			let proc = Bun.spawn([
				"target/debug/tg",
				"--mode=server",
				`--config=${configPath}`,
				"publish",
				"-p",
				`${sourcePath}/${name}/dependencies/${dependency}`,
			]);
			await proc.exited;
			expect(proc.exitCode).toBe(0);
		} catch (e) {
			console.log(e);
		}
	}
	console.log(`tg tree ${name}/dependant`);
	let proc = Bun.spawn([
		"target/debug/tg",
		"--mode=server",
		`--config=${configPath}`,
		"tree",
		`${sourcePath}/${name}/dependant`,
	]);
	await proc.exited;
	expect(proc.exitCode).toBe(0);
	let text = await Response(proc.stdout).text();
	expect(text).toBe(expected);
};

// Test case: can we resolve a simple diamond dependency.
test("diamondDependencies", async () => {
	let dependencies = ["qux", "baz", "bar", "foo"];
	let expected = `${sourcePath}/diamondDependencies/dependant: dependant@0.0.0
└── foo@diamond: foo@diamond
    ├── bar@diamond: bar@diamond
    │   └── qux@diamond: qux@diamond
    └── baz@diamond: baz@diamond
        └── qux@diamond: qux@diamond
`;
	await runTest("diamondDependencies", dependencies, expected);
});

// Test case: do we resolve the latest version.
test("latestVersion", async () => {
	let dependencies = [
		"latest@1.0.0",
		"latest@1.1.0",
		"latest@1.2.0",
		"latest@1.3.0",
	];
	let expected = `${sourcePath}/latestVersion/dependant: dependant@0.0.0
└── latest@^1: latest@1.3.0
`;
	await runTest("latestVersion", dependencies, expected);
});

// Test case: can we resolve path dependencies.
test("pathDependencies", async () => {
	let dependencies = [];
	let expected = `${sourcePath}/pathDependencies/dependant: dir_010yqma3msk352ck277z7dryj61hzn6sh4z45ptsnb3rg4h3441d80
├── ../dependencies/foo: dir_01yceqcsfgzjcghftaacm7n3h7a4xd7r2j9t82x0jqesydngjqdrtg
└── {"name":"bar","path":"./bar"}: bar
`;
	await runTest("pathDependencies", dependencies, expected);
});

// Test case: can we resolve nested path dependencies in a registry dependency.
test("nestedPathDependencies", async () => {
	let dependencies = ["root"];
	let expected = `${sourcePath}/nestedPathDependencies/dependant: dependant@0.0.0
└── root: root@0.0.0
    ├── ./child1: dir_0115c9y3w4g7qaxbxg3cg7d7ye3vhhfxd0xvr3ja5ge1eqb4srrceg
    │   └── ../child2: dir_01a5bxg7w3tvnbm68m2wrb5a48pha5bxt2tegzny8rhcdnqxv61ymg
    │       └── ./grandchild: dir_01yceqcsfgzjcghftaacm7n3h7a4xd7r2j9t82x0jqesydngjqdrtg
    └── ./child2: dir_01a5bxg7w3tvnbm68m2wrb5a48pha5bxt2tegzny8rhcdnqxv61ymg
`;
	await runTest("nestedPathDependencies", dependencies, expected);
});

// Test case: does backtracking work as expected.
test("simpleBacktracking", async () => {
	let dependencies = ["baz@2.0.0", "baz@2.1.0", "bar@2.0.0", "bar@2.1.0"];
	let expected = `${sourcePath}/simpleBacktracking/dependant: dependant@0.0.0
├── bar@^2: bar@2.0.0
│   └── baz@=2.0.*: baz@2.0.0
└── baz@=2.0.*: baz@2.0.0
`;
	await runTest("simpleBacktracking", dependencies, expected);
});
