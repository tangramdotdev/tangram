import { context } from "../../scripts/context.tg.ts";

export const check = async () => {
	const { cwd, env } = await context("packages/python");
	await tg.run`uv run --locked python -m ruff check . src && uv run --locked ty check src && uv run --locked python -X pycache_prefix=../../target/pycache -m compileall -q src`
		.cwd(cwd)
		.env(env);
};

export const format = async () => {
	const { cwd, env } = await context("packages/python");
	await tg.run`uv run --locked python -m ruff format .`.cwd(cwd).env(env);
};
