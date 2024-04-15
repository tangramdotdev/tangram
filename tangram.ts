import * as testVersionSolving from "./packages/tests/versionSolving.tg.ts";
export default tg.target(async () => {
	return tg.file("Hello, World!");
});

export let testVersionSolvingSource = tg.target(() => testVersionSolving.source());
