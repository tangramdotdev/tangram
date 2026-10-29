use ../../../lib/test.nu *

# tg.blob with more children than fit in a branch nests them in branches, like a blob written by checkin.

let local = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			let texts = [];
			let children = [];
			for (let i = 0; i < 1025; i++) {
				texts.push(String(i));
				children.push(await tg.blob(String(i)));
			}
			let blob = await tg.blob(...children);
			let object = await blob.object();
			return [object.children.length, (await blob.text) === texts.join("")];
		}
	'
}
let output = tg build $path
snapshot $output '[2,true]'
