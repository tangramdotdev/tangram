use ../../test.nu *

# A token authorizes a resource only when it names that exact resource. Anything else falls back to
# walking the object graph, and this measures what that walk costs. Two things drive it: graph
# objects arrive with no token at all, and directories arrive with one that names their root, which
# the walk has to reach. It ends on a build that fails because a directory node sits further from
# its token than the walk can go. The last assertion is that the build succeeds, so this fails
# until it is.

# The artifact is two packages importing each other, forming one graph object, plus a directory
# built to exceed both search limits at once. Both dimensions are needed: depth alone or width
# alone still authorizes, because either search direction can settle the resource on its own.
let levels = 18
let fanout = 64

let server = server spawn --config {
	tracing: {
		filter: 'tangram_server::authorization=debug,tangram_index::lmdb::authorize=debug,tangram_server::http=trace'
		stderr_format: 'json'
	}
}

let module = r#'
	import * as other from "../other";
	export function shared() { return "shared"; }
	export async function big(): Promise<tg.Directory> {
		let dir = await tg.directory({ "leaf.txt": "found" });
		for (let i = 0; i < LEVELS; i++) {
			let entries: any = { "nested": dir };
			for (let j = 0; j < FANOUT; j++) { entries[`f${i}_${j}.txt`] = `x${i}_${j}`; }
			dir = await tg.directory(entries);
		}
		return dir;
	}
	export async function pick(): Promise<tg.File> {
		let dir = await tg.build(big) as tg.Directory;
		let path = Array(LEVELS).fill("nested").join("/") + "/leaf.txt";
		return await dir.get(path) as tg.File;
	}
	export default async function (): Promise<string> {
		await tg.build(other.entry);
		let file = await tg.build(pick) as tg.File;
		return await file.text;
	}
'# | str replace --all 'LEVELS' ($levels | into string) | str replace --all 'FANOUT' ($fanout | into string)

let path = artifact {
	pkg: {
		tangram.ts: $module
	}
	other: {
		tangram.ts: r#'
			import * as back from "../pkg";
			export async function entry(): Promise<string> { return await tg.build(back.shared); }
		'#
	}
}

let result = do { tg build ($path | path join './pkg') } | complete

let events = open $server.log
	| lines
	| where ($it | str starts-with '{')
	| each { |line| $line | from json }

# Every authorize argument passes through the token check, so it is the denominator. Each carries
# the route it arrived on, taken from the innermost HTTP span that names a path.
let requests = $events
	| where { |e| $e.fields.message? == 'authorize token' }
	| each { |e| {
		id: ($e.fields.resource? | default '')
		kind: ($e.fields.resource? | default '' | str substring 0..2)
		outcome: ($e.fields.outcome? | default '')
		token: ($e.fields.token_resource? | default '')
		route: ($e.spans? | default []
			| where { |s| ($s.path? | default '') != '' }
			| get path? | last | default '-'
			| str replace --regex --all '[a-z]{3}_[0-9a-z]{20,}' '<id>')
	} }
let absent = $requests | where outcome == 'absent'

# What each completed search visited, how long it took, and what finally proved it: a token or a
# grant found some number of hops up from the resource, or a result already cached in this batch.
let searches = $events
	| where { |e| $e.fields.message? == 'authorize search' }
	| each { |e|
		let f = $e.fields
		{
			id: ($f.resource? | default '')
			kind: ($f.resource? | default '' | str substring 0..2)
			nodes: (($f.ancestor_nodes? | default 0) + ($f.descendant_nodes? | default 0))
			edges: (($f.ancestor_edges? | default 0) + ($f.descendant_edges? | default 0))
			ms: (($f.duration? | default 0.0) * 1000)
			proof: ($f.proof? | default '-')
			hops: ($f.proof_depth? | default 0)
		}
	}

# An object is reached through the module graph if any request for it arrived on a module
# resolution route. Index searches run off the request thread and carry no route of their own, so
# they are attributed by resource id.
let through_graph = $requests | where route in ['/modules/load' '/modules/resolve'] | get id | uniq

def totals [rows: list] {
	{
		searches: ($rows | length)
		objects: ($rows | get id | uniq | length)
		nodes: ($rows | get nodes | append 0 | math sum)
		edges: ($rows | get edges | append 0 | math sum)
		ms: ($rows | get ms | append 0 | math sum | math round --precision 1)
	}
}

print $'artifact: ($levels) nesting levels of ($fanout) files each, ($levels * $fanout + $levels) objects in the directory'
print $'          ($levels) levels exceeds the depth cap of 16; ($levels * $fanout) files exceeds the node cap of 1024'
print $'requests ($requests | length), of which ($absent | length) carried no token'

# Why each search ran. An exact token names the resource and settles it without a search. A present
# one names something else, so it only helps once the walk reaches whatever it does name.
print ''
print 'token outcome by resource kind'
$requests | group-by kind | items { |k, v| {
	kind: $k
	requests: ($v | length)
	exact: ($v | where outcome == 'exact' | length)
	present: ($v | where outcome == 'present' | length)
	absent: ($v | where outcome == 'absent' | length)
} } | sort-by requests --reverse | print

print ''
print 'search cost by resource kind'
$searches | group-by kind | items { |k, v| { kind: $k } | merge (totals $v) }
	| sort-by ms --reverse | print

print ''
print 'search cost by whether the object is reached through the module graph'
$searches | group-by { |x| if ($x.id in $through_graph) { 'through-graph' } else { 'direct' } }
	| items { |k, v| { reached: $k } | merge (totals $v) } | sort-by ms --reverse | print

# How far the index walked before something proved the request. A token or grant found more than
# zero hops up is one that existed but did not name the resource that was asked for.
print ''
print 'what proved each search, and how far up it was found'
$searches | group-by proof | items { |k, v| {
	proof: $k
	searches: ($v | length)
	at_0: ($v | where hops == 0 | length)
	at_1_2: ($v | where hops > 0 and hops <= 2 | length)
	at_3_plus: ($v | where hops > 2 | length)
	furthest: ($v | get hops | append 0 | math max)
	nodes: ($v | get nodes | append 0 | math sum)
	ms: ($v | get ms | append 0 | math sum | math round --precision 1)
} } | sort-by searches --reverse | print

let indirect = $searches | where hops > 0
print $'($indirect | length) of ($searches | length) searches were proved by something further up the graph than the resource asked for, costing ($indirect | get ms | append 0 | math sum | math round --precision 1) ms'

let worst = $searches | group-by id | items { |k, v| {
	id: ($k | str substring 0..6)
	searches: ($v | length)
	requests: ($requests | where id == $k | length)
	covered: ($requests | where id == $k and outcome == 'exact' | length)
	ms: ($v | get ms | append 0 | math sum | math round --precision 1)
	kind: ($k | str substring 0..2)
} } | sort-by searches --reverse | first
print ''
print $'most searched object: ($worst.id), ($worst.searches) searches over ($worst.requests) requests, ($worst.covered) covered by a token, ($worst.ms) ms'

# Searches that ran out in both directions. The initial pass runs with the descendant search
# disabled, so only an event with a descendant budget is one that fails a build.
let exhausted = $events
	| where { |e| $e.fields.message? == 'authorize search exhausted' }
	| where { |e| ($e.fields.descendant_max_nodes? | default 0) > 0 }
	| each { |e| $e.fields }
print ''
print $'searches that ran out in both directions: ($exhausted | length)'
$exhausted | each { |f|
	print $'  ($f.resource | str substring 0..6) ($f.permission), ((($f.duration? | default 0.0) * 1000) | math round --precision 1) ms spent before failing'
	print $'    ancestor   depth ($f.ancestor_deepest)/($f.ancestor_max_depth), nodes ($f.ancestor_nodes)/($f.ancestor_max_nodes), edges ($f.ancestor_edges)/($f.ancestor_max_edges), ($f.ancestor_pending) unexplored'
	print $'    descendant nodes ($f.descendant_nodes)/($f.descendant_max_nodes), edges ($f.descendant_edges), ($f.descendant_pending) unexplored'
	let token = $requests | where id == $f.resource | get token | first | default ''
	let peers = $requests | where token == $token and id != $f.resource | get id | uniq
	let proved = $searches | where { |x| $x.id in $peers and $x.proof == 'token' }
	print $'    a token naming ($token | str substring 0..6) was presented for it, and the search never reached it'
	if ($proved | is-not-empty) {
		print $'    that same token proved ($proved | length) other requests, at hops ($proved | get hops | math min) through ($proved | get hops | math max)'
	}
} | ignore

# Graph objects and the blobs reached through them arrive with no token at all, so every request
# for one pays a search.
let graph = $requests | where { |r| $r.kind in ['gph' 'blb'] }
assert equal ($graph | where outcome == 'exact' | length) 0
assert (($graph | where outcome == 'absent' | length) * 2 > ($graph | length))

# Directories always carry a token, but it names their root rather than the node asked for, so it
# only authorizes once the search walks to it. Tokens are redeemed as far up as the depth cap and
# no further, which is what fails the build below.
let directories = $requests | where kind == 'dir'
assert equal ($directories | where outcome == 'absent' | length) 0
assert (($directories | where outcome == 'present' | length) > ($directories | where outcome == 'exact' | length))
assert equal ($searches | where proof == 'token' | get hops | math max) 16

# The most searched resource is the graph object formed by the import cycle. No token ever covered
# it, and it is searched again on every request rather than once.
assert equal $worst.kind 'gph'
assert equal $worst.covered 0
assert ($worst.searches > 20)
assert ($worst.searches > $worst.requests)

# The searches ran out in both directions, which is what fails the build.
assert (($exhausted | length) > 0)

# The build should succeed. It does not, because that resource could not be authorized.
assert equal $result.exit_code 0
