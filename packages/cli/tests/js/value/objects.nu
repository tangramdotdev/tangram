use ../../../test.nu *

# tg.Value.objects collects the objects nested anywhere within a value.

let server = spawn

let path = artifact {
	tangram.ts: 'export default async function () { return tg.Value.objects({ a: await tg.file("hi"), b: 1 }); }'
}

let output = tg build $path
snapshot --normalize-ids $output '[fil_010000000000000000000000000000000000000000000000000000]'
