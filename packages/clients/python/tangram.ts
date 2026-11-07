import { context } from "../../../scripts/context.tg.ts";

export const check = async () => {
	const { cwd, env } = await context("packages/clients/python");
	await tg.run`uv run --locked --package tangram python -m ruff check . && uv run --locked --package tangram ty check src && uv run --locked --package tangram python -X pycache_prefix=../../../target/pycache -m compileall -q src`
		.cwd(cwd)
		.env(env);
};

export const format = async () => {
	const { cwd, env } = await context("packages/clients/python");
	await tg.run`uv run --locked --package tangram python -m ruff format .`
		.cwd(cwd)
		.env(env);
};
