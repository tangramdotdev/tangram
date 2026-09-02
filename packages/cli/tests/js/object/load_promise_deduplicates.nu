use ../../../test.nu *

# Concurrent loads share one promise per object state.

let server = server spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			const id = "blb_010000000000000000000000000000000000000000000000000000" as tg.Blob.Id;
			const getObject = tg.client.getObject;
			let calls = 0;
			try {
				tg.client.getObject = async () => {
					calls += 1;
					await tg.sleep(0.05);
					return { data: { kind: "blob", value: { bytes: "" } } };
				};
				const blob = tg.Blob.withId(id);
				await Promise.all([blob.load(), blob.load(), blob.object()]);

				return calls;
			} finally {
				tg.client.getObject = getObject;
			}
		}
	'
}

let output = tg build $path
snapshot $output '1'
