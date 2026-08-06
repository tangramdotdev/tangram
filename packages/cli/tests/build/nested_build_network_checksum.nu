use ../../test.nu *

# A child build that provides a checksum may enable the network even though its parent build's sandbox does not have the network enabled.

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.build`true`
				.network(true)
				.checksum("sha256:any");
		}
	',
}

success (tg build $path | complete)
