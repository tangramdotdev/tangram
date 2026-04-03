use ../../test.nu *

let server = spawn

let path = artifact {
	tangram.ts: '
		export default async function () {
			return await tg.run(inspect, {
				cwd: tg.process.cwd,
				output: tg.process.env.TANGRAM_OUTPUT,
				url: tg.process.env.TANGRAM_URL,
			});
		}

		export function inspect(parent) {
			return JSON.stringify({
				cwdMatches: tg.process.cwd === parent.cwd,
				urlMatches: tg.process.env.TANGRAM_URL === parent.url,
				outputChanged:
					typeof parent.output === "string" &&
					typeof tg.process.env.TANGRAM_OUTPUT === "string" &&
					tg.process.env.TANGRAM_OUTPUT !== parent.output,
			});
		}
	',
}

let output = do { cd $path; tg run } | from json | from json
assert $output.cwdMatches
assert $output.urlMatches
assert $output.outputChanged
