import { $ } from "tg:std" with { path: "../../../packages/packages/std" };
import { env } from "./tangram.ts";

export default tg.target(async () => {
	let tangramTs = (await import("./src/logs.ts", { with: { type: "file" } }))
		.default;
	return await $`
    cp ${tangramTs} tangram.ts
    BLD_JS=$(tg build --no-tui -t js 2>&1 | grep -o 'bld_[0-9a-z]*')
    BLD_SHELL=$(tg build --no-tui -t $(tg build --no-tui -t shell) 2>&1 | grep -o 'bld_[0-9a-z]*')
    mkdir -p $OUTPUT
    cp /tmp/.tangram/logs/$BLD_JS $OUTPUT/js
    cp /tmp/.tangram/logs/$BLD_SHELL $OUTPUT/shell
  `
		.env(env())
		.then(tg.Directory.expect);
});
