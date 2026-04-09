use ../../test.nu *

# Reproduces the macOS sandbox bug: when the server's data directory is under
# $HOME (like the default ~/.tangram), the sandbox profile does not grant access
# to checked-out artifacts. Passes when artifacts are under /private/var (temp).

let dir = ($env.HOME | path join '.tangram_test_sandbox')
mkdir $dir
let server = spawn -d $dir

let path = artifact {
	tangram.ts: '
		export default async () => {
			let tool = tg.file({ contents: "#!/bin/sh\necho hello\n", executable: true });
			return tg.build`${tool} > ${tg.output}`;
		}
	',
}

let id = tg build $path
let output = tg checkout $id
snapshot (open $output) '
	hello
'

rm -rf $dir
