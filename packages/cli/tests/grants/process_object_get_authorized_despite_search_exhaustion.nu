use ../../test.nu *

const js_path = path self '../../../js'

# The subtree proof is command -> source -> proof parent -> target.
# Either search succeeds when only the opposite frontier is wide; both exhaust when both are wide.
#
#     Search       Start    Irrelevant frontier before the proof parent
#     ancestor     target   2,499 other parents
#     descendant   source   2,500 decoy children

let root_token = random chars
let server = server spawn --config {
	authentication: { root: { token: $root_token } }
	index: { kind: 'lmdb', map_size: 1_073_741_824 }
}

let setup_program = '
	import * as tg from "@tangramdotdev/client";

	const decoder = new TextDecoder();
	const encoder = new TextEncoder();
	tg.setEncoding({
		json: { decode: JSON.parse, encode: JSON.stringify },
		utf8: {
			decode: (value) => decoder.decode(value),
			encode: (value) => encoder.encode(value),
		},
	});
	const env = Object.fromEntries(
		Object.entries(process.env).filter(([, value]) => value !== undefined),
	);
	tg.setProcess({
		args: process.argv.slice(2),
		cwd: process.cwd(),
		env,
		executable: process.execPath,
	});

	function directory(entries) {
		const data = { kind: "directory", value: { entries } };
		return { data, id: tg.client.objectId(data) };
	}

	function batchObject(object, children) {
		return {
			children: children.map((child) => tg.Object.toReferent(child)),
			data: object.data,
			id: object.id,
		};
	}

	const target = tg.File.withId(env.TARGET);
	const leaf = tg.File.withId(env.LEAF);
	const parents = Array.from({ length: 2_500 }, (_, index) =>
		directory({ [`target${index}`]: target.id }),
	).sort((a, b) => a.id.localeCompare(b.id));
	const proofParent = parents.pop();
	const targetParents = env.MANY_PARENTS === "true"
		? [...parents, proofParent]
		: [proofParent];
	await tg.client.postObjectBatch({
		objects: targetParents.map((parent) => batchObject(parent, [target])),
	});

	const decoys = [];
	if (env.MANY_CHILDREN === "true") {
		for (let index = 0; decoys.length < 2_500; index++) {
			const decoy = directory({ [`decoy${index}`]: leaf.id });
			if (decoy.id < proofParent.id) decoys.push(decoy);
		}
		await tg.client.postObjectBatch({
			objects: decoys.map((decoy) => batchObject(decoy, [leaf])),
		});
	}
	const entries = Object.fromEntries(
		decoys.map((decoy, index) => [`decoy${index}`, decoy.id]),
	);
	entries.proof = proofParent.id;
	const sourceData = {
		kind: "graph",
		value: { nodes: [{ kind: "directory", entries }] },
	};
	const source = { data: sourceData, id: tg.client.objectId(sourceData) };
	const children = decoys.map((decoy) => tg.Directory.withId(decoy.id));
	children.push(tg.Directory.withId(proofParent.id));
	await tg.client.postObjectBatch({
		objects: [batchObject(source, children)],
	});

	process.stdout.write(source.id);
	process.exit(0);
'

let build_program = '
	export default async (_source: tg.Graph, target: string) => {
		await tg.File.withId(target).state.load();
		return "ok";
	}
'

def run_case [
	setup_program: string
	build_program: string
	root_token: string
	target: string
	leaf: string
	--many-parents
	--many-children
] {
	let setup = with-env {
		LEAF: $leaf
		MANY_CHILDREN: ($many_children | into string)
		MANY_PARENTS: ($many_parents | into string)
		TANGRAM_TOKEN: $root_token
		TARGET: $target
	} {
		node --input-type=module -e $setup_program | complete
	}
	success $setup 'the authorization graph should be created'
	tg --token $root_token index
	let source = $setup.stdout | str trim
	let module = artifact { tangram.ts: $build_program }

	tg --token $root_token build $module --arg-value $source --arg-string $target | complete
}

let parent_target = tg --token $root_token put 'tg.file("many parent target")' | str trim
let source_target = tg --token $root_token put 'tg.file("wide source target")' | str trim
let combined_target = tg --token $root_token put 'tg.file("combined target")' | str trim
let leaf = tg --token $root_token put 'tg.file("wide leaf")' | str trim

cd $js_path

# Many target parents alone succeed because the descendant search reaches the proof.
let output = run_case $setup_program $build_program $root_token $parent_target $leaf --many-parents
success $output 'many target parents alone should not prevent the process from reading the target'

# Many source children alone succeed because the ancestor search reaches the proof.
let output = run_case $setup_program $build_program $root_token $source_target $leaf --many-children
success $output 'a wide command subtree alone should not prevent the process from reading the target'

# Combining both frontiers exhausts both bounded searches during the process object read.
let output = run_case $setup_program $build_program $root_token $combined_target $leaf --many-children --many-parents
server stop $server
assert ($output.stderr | str contains 'failed to get the object') 'the combined case should reach the process object read'
assert ($output.stderr | str contains 'the authorization search exhausted') 'the combined case should exhaust both authorization searches'
failure $output 'a tokenless read should remain bounded when both authorization searches exhaust'
