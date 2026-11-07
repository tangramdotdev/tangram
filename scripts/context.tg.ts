export const context = async (directory = ".") => {
	let root = tg.process.cwd;
	while (!(await tg.host.exists(tg.path.join(root, "rust-toolchain.toml")))) {
		const parent = tg.path.parent(root);
		if (!parent || parent === root) {
			throw new Error("run repository scripts from within the repository");
		}
		root = parent;
	}
	const cwd = tg.path.join(root, directory);
	const path = [
		tg.path.join(cwd, "node_modules/.bin"),
		tg.path.join(root, "node_modules/.bin"),
	].join(":");
	const env = { PATH: await tg.Mutation.prefix(path, ":") };
	return { cwd, env };
};
