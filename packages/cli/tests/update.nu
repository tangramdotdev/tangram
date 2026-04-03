use ../test.nu *

let server = spawn

# Create the transitive dependency (version 1).
let transitive_1_0_0 = artifact {
	tangram.ts: '
		export default () => "transitive v1";
	'
}
tg tag transitive/1.0.0 $transitive_1_0_0
tg index
# Create the direct dependency (version 1) which depends on transitive.
let dep_1_0_0 = artifact {
	tangram.ts: '
		import transitive from "transitive/^1.0";
		export default () => transitive();
	'
}
tg tag dep/1.0.0 $dep_1_0_0
tg index
# Create a dependency that will be removed in the next version.
let removed = artifact {
	tangram.ts: '
		export default () => "removed";
	'
}
tg tag removed/1.0.0 $removed
tg index
# Create the initial root package which depends on dep and removed.
let old_root = artifact {
	tangram.ts: '
		import dep from "dep/^1.0";
		import removed from "removed/^1";
		export default () => dep();
	'
}

# Check in to create the initial lockfile.
tg checkin $old_root

# Create updated versions.
let transitive_1_1_0 = artifact {
	tangram.ts: '
		export default () => "transitive v2";
	'
}
tg tag transitive/1.1.0 $transitive_1_1_0
tg index

# Create a new dependency that will be added.
let added_path = artifact {
	tangram.ts: '
		export default () => "added";
	'
}
tg tag added/1.0.0 $added_path
tg index

let dep_1_1_0 = artifact {
	tangram.ts: '
		import transitive from "transitive/^1.0";
		import added from "added/^1";
		export default () => transitive();
	'
}
tg tag dep/1.1.0 $dep_1_1_0
tg index

# Create the updated root package which drops removed and adds added.
let new_root = artifact {
	tangram.ts: '
		import dep from "dep/^1.0";
		import added from "added/^1";
		export default () => dep();
	'
}

# Copy the lockfile from the initial root to the new root so the update can diff against it.
cp ($old_root | path join "tangram.lock") ($new_root | path join "tangram.lock")

# Run update and capture the output.
let output = do --env {
	cd $new_root
	tg update .
} | complete
success $output
let stdout = (
	$output.stdout
	| str trim
)
snapshot $stdout '
	+ added added/1.0.0, required by tangram.ts
	+ added added/1.0.0, required by dep/1.1.0/tangram.ts
	↑ updated dep/1.0.0 to dep/1.1.0, required by tangram.ts
	- removed removed/1.0.0, required by tangram.ts
	- removed transitive/1.0.0, required by dep/1.0.0/tangram.ts
	+ added transitive/1.1.0, required by dep/1.1.0/tangram.ts
'
