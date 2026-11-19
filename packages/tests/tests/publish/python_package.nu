use ../../util/test.nu *

# Publishing a Python package reads its tag and builds from its metadata dict without running it, for a single-file package and for a directory package.

let host = current_host
let remote = server spawn --name remote
let local = server spawn --name local --config {
	remotes: { default: { url: $remote.url } }
}

# Create a Python module whose metadata has the given tag and builds the default export on this host.
def python-module [tag: string, host: string] {
	[
		'metadata: dict = {'
		$'    "tag": "($tag)",'
		$'    "builds": {"hosts": ["($host)"], "exports": ["default"], "public": False},'
		'    "license": "MIT",'
		'}'
		''
		'def default():'
		'    return "python"'
	] | str join "\n"
}

# A single-file package.
let directory = mktemp --directory
let file = $directory | path join 'package.tg.py'
python-module python-file/1.0.0 $host | save $file

# A directory package.
let package = artifact { tangram.py: (python-module python-directory/1.0.0 $host) }

for case in [[path name]; [$file python-file] [$package python-directory]] {
	let tag = $'($case.name)/1.0.0'
	let build = $'($case.name)/builds/1.0.0/default/($host)'

	# A dry run lists the package tag and its build tags.
	let output = tg publish --dry-run $case.path | complete
	success $output "the dry run should succeed"
	let plan = $output.stdout | from json
	assert equal ($plan | get 0.tag) $tag "the plan should list the package tag"
	assert equal ($plan | get 0.builds.tag) [$build] "the plan should list the build tags"
	failure (tg tag get $tag | complete) "a dry run should not create the tag"

	# A publish tags the package and its build on the remote.
	let output = tg publish $case.path | complete
	success $output "the publish should succeed"
	success (tg --url $remote.url tag get $tag | complete) "the remote should have the package tag"
	let process = tg --url $remote.url tag get $build | from json | get target.id
	assert equal (tg --url $remote.url process output $process | from json) "python"
}
