use ../../../test.nu *

# tg.Checksum.is accepts well-formed checksums and rejects unsupported algorithms, malformed strings, and non-strings.

let server = spawn

let path = artifact {
	tangram.ts: 'export default function () { return [tg.Checksum.is("sha256:2cf24dba"), tg.Checksum.is("notachecksum"), tg.Checksum.is("md5:abc"), tg.Checksum.is(42)]; }'
}

let output = tg build $path
snapshot $output '[true,false,false,false]'
