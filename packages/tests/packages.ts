import { $ } from "tg:std" with { path: "../../../packages/packages/std" };
import { env } from "./tangram.ts";

export default tg.target(() =>
	tg.directory({
		"diamondDependencies/tangram.lock": runTest(
			diamondDependenciesSource(),
			"qux",
			"baz",
			"bar",
			"foo",
		),
		"latestVersion/tangram.lock": runTest(
			latestVersionSource(),
			"latest@1.0.0",
			"latest@1.1.0",
			"latest@1.2.0",
			"latest@1.3.0",
		),
		"nestedPathDependencies/tangram.lock": runTest(
			nestedPathDependenciesSource(),
			"",
		),
		"simpleBacktracking/tangram.lock": runTest(
			simpleBacktrackingSource(),
			"baz@2.0.0",
			"baz@2.1.0",
			"bar@2.0.0",
			"bar@2.1.0",
		),
	}),
);

export let runTest = tg.target(
	async (source: tg.Directory, ...dependencies: Array<string>) => {
		let script = tg`
		cp -r ${source}/. .
		chmod -R +w .
	`;

		for (let i = 0; i < dependencies.length; i++) {
			let dependency = dependencies[i];
			script = tg`${script}
			timeout 10 tg publish -p ./dependencies/${dependency}
		`;
		}
		script = tg`${script}
		echo "checking dependendant"
		timeout 10 tg check -p ./dependant || true
		echo "ok"
		mv ./dependant/tangram.lock $OUTPUT
	`;
		return await $`${script}`.env(env()).then(tg.File.expect);
	},
);

export let diamondDependenciesSource = tg.target(() =>
	tg.directory({
		"dependant/tangram.ts": `
		import * as foo from "tg:foo@diamond";
		export let metadata = {
			name: "dependant",
			version: "0.0.0",
		};
	`,
		"dependencies/foo/tangram.ts": `
		import * as bar from "tg:bar@diamond";
		import * as baz from "tg:baz@diamond";
		export let metadata = {
			"name": "foo",
			"version": "diamond",
		};
	`,
		"dependencies/bar/tangram.ts": `
		import * as qux from "tg:qux@diamond";
		export let metadata = {
			"name": "bar",
			"version": "diamond",
		};
	`,
		"dependencies/baz/tangram.ts": `
		import * as qux from "tg:qux@diamond";
		export let metadata = {
			"name": "baz",
			"version": "diamond",
		};
	`,
		"dependencies/qux/tangram.ts": `
		export let metadata = {
			"name": "qux",
			"version": "diamond",
		};
	`,
	}),
);

export let latestVersionSource = tg.target(() =>
	tg.directory({
		"dependant/tangram.ts": `
		import * as latest from "tg:latest@^1";
		export let metadata = {
			name: "dependant",
			version: "0.0.0",
		};
	`,
		"dependencies/latest@1.0.0/tangram.ts": `export let metadata = { name: "latest", version: "1.0.0" };`,
		"dependencies/latest@1.1.0/tangram.ts": `export let metadata = { name: "latest", version: "1.1.0" };`,
		"dependencies/latest@1.2.0/tangram.ts": `export let metadata = { name: "latest", version: "1.2.0" };`,
		"dependencies/latest@1.3.0/tangram.ts": `export let metadata = { name: "latest", version: "1.3.0" };`,
	}),
);

export let nestedPathDependenciesSource = tg.target(() =>
	tg.directory({
		"dependant/tangram.ts": `
		import * as root from "tg:root";
		export let metadata = {
			name: "dependant",
			version: "0.0.0",
		};
	`,
		"dependencies/tangram.ts": `
		import * as child1 from "tg:./child1";
		import * as child2 from "tg:./child2";
		export let metadata = {
			name: "root",
			version: "0.0.0",
		}
	`,
		"dependencies/child1": {
			"tangram.ts": `
			import * as sibling from "tg:../child2";
		`,
		},
		"dependencies/child2": {
			"tangram.ts": `
			import * as grandchild from "tg:./grandchild";
		`,
			grandchild: {
				"tangram.ts": "",
			},
		},
	}),
);

/*
	Demonstrates a simple case where backtracking is required.

	There is a diamond dependency:

	foo * <- bar <- * - baz
			\          /
			 *<-------*

	foo requires bar@2 and baz@2.0. There are two candidates for bar and baz:
		bar@2.0.0
			requires baz@^2
		bar@2.1.0
			requires baz@2.1
		baz@2.0.0
		baz@2.1.0

	the version solving algorithm should take these steps:

	step  pkg          dep           solution    result
				<begin>
	1     foo       <- bar@^2    :=  bar@2.0.0   OK   continue
	2     bar@2.1.0 <- baz@^2.1  :=  baz@2.1.0   OK   continue
	3     foo       <- baz@^2.0  :=  baz@2.1.0   ERR  backtrack until baz is not in solution
	3     bar@2.0.0 <- baz@^2.1  :=  none        ERR  backtrack until bar is not in solution
	4     foo       <- bar@^2    :=  bar@2.0.0   OK   continue
	5     bar@2.0.0 <- baz@^2.1  :=  baz@2.1.0   OK   continue
	6     foo       <- baz@2.0   :=  baz@2.1.0   ERR  backtrack until baz is not in solution
	7     bar@2.0.0 <- baz@^2    :=  baz@2.0.0   OK   continue
	8     foo       <- baz@^2.0  :=  baz@2.0.0   OK   continue
	9     <end>

	foo
	├── bar@^2: bar@2.0.0
	│   └── baz@=2.0.*: baz@2.0.0
	└── baz@=2.0.*: baz@2.0.0
*/
export let simpleBacktrackingSource = tg.target(() =>
	tg.directory({
		"dependant/tangram.ts": `
		import * as bar from "tg:bar@^2";
		import * as baz from "tg:baz@=2.0.*";
		export let metadata = {
			name: "dependant",
			version: "0.0.0",
		};
	`,
		"dependencies/bar@2.0.0/tangram.ts": `
		import * as baz from "tg:baz@=2.0.*"
		export let metadata = {
			name: "bar",
			version: "2.0.0"
		};
	`,
		"dependencies/bar@2.1.0/tangram.ts": `
		import * as baz from "tg:baz@=2.1.*"
		export let metadata = {
			name: "bar",
			version: "2.1.0"
		};
	`,
		"dependencies/baz@2.0.0/tangram.ts": `
		export let metadata = {
			name: "baz",
			version: "2.0.0"
		};
	`,
		"dependencies/baz@2.1.0/tangram.ts": `
		export let metadata = {
			name: "baz",
			version: "2.1.0"
		};
	`,
	}),
);
