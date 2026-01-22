use ../../test.nu *

let server = spawn
let path = artifact {
	tangram.ts: '
		import a from "a/^1";
	'

}

# Verify that we cannot check this in with --deterministic set.
let output = tg checkin $path --deterministic | complete
failure $output

# Verify we can check this in with --deterministic and --unsolved-dependencies
let id = tg checkin $path --deterministic --unsolved-dependencies
tg index
let object = tg get --depth=inf --pretty $id
snapshot $object '
	tg.directory({
	  "tangram.ts": tg.file({
	    "contents": blb_01mzk6yctk6vb8f7k35qw07218x9mv26x7kaxq78ynd1ym1an10x8g,
	    "dependencies": {
	      "a/^1": null,
	    },
	    "module": "ts",
	  }),
	})
';

