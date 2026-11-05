use ../../util/test.nu *
use ../../util/vfs.nu

# The client-backed VFS traverses directories and graphs, reads blobs and symlinks, and exports dependency proofs.
vfs skip_unless_supported

for kind in [fjall lmdb] {
	let server_path = mktemp --directory
	let local = server spawn --directory $server_path --config {
		cache: { kind: $kind }
		vfs: true
	}
	let module = artifact {
		tangram.ts: '
			export default async () => {
				const dependency = await tg.file("dependency\n");
				const file = await tg.file({ contents: "contents\n", dependencies: { dependency }, module: "typescript" });
				const graph = await tg.graph({ nodes: [
					{ kind: "directory", entries: { link: 2, self: 0, value: 1 } },
					{ kind: "file", contents: "graph\n" },
					{ kind: "symlink", artifact: 1 },
				] });
				const directory = await tg.directory({ graph, index: 0, kind: "directory" });
				const root = await tg.directory({ nested: tg.directory({ file }), graph: directory, link: tg.symlink({ artifact: file }) });
				await root.store();
				return { dependency: dependency.id, directory: root.id, file: file.id };
			};
		'
	}
	let artifacts = tg build $module | from json
	let root = vfs root $server_path $artifacts.directory
	for path in ['nested/file' link] {
		assert equal (open --raw ($root | path join $path)) "contents\n"
	}
	for path in ['graph/value' 'graph/link' 'graph/self/self/link'] {
		assert equal (open --raw ($root | path join $path)) "graph\n"
	}
	let file_path = $root | path join nested file
	assert equal (xattr_read user.tangram.module $file_path) typescript
	let references = xattr_read user.tangram.dependencies $file_path | from json
	let token_params = $'http://localhost/($references | first)' | url parse | get params | where key =~ '^tokens'
	assert (not ($token_params | is-empty)) 'the dependency reference must carry its authorization proofs'
	let bodies = $token_params | each { |entry| $entry.value | token body }
	assert ($bodies | any { |body| $body.resource == $artifacts.dependency and ('object_subtree' in $body.permissions) }) 'the reference must authorize the dependency subtree'
	let file_body = xattr_read user.tangram.token $file_path | token body
	assert equal $file_body.resource $artifacts.file
	assert ('object_subtree' in $file_body.permissions) 'the file xattr must carry a subtree proof' 
	server stop $local
}
