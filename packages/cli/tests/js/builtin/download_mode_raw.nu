use ../../../test.nu *

# tg.download accepts a "raw" mode option, downloading the contents without any postprocessing.

skip_if_offline

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async () => {
			let blob = await tg.download("http://www.example.com", undefined, { mode: "raw" });
			return (blob instanceof tg.Blob) && (await blob.length) > 0;
		};
	'
}

let output = tg build $path
snapshot $output 'true'
