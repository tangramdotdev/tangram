export let metadata = {
	name: "tangram",
	version: "0.0.0",
};

export default tg.target(async () => {
	return [f(), g(), h()];
});

export let f = tg.target(async () => {
	console.log("starting");
	await tg.sleep(5);
	return "Hello, World!";
});

export let g = tg.target(async () => {
	console.log("starting");
	await tg.sleep(5);
	return "Hello, World!";
});

export let h = tg.target(async () => {
	console.log("starting");
	await tg.sleep(5);
	return "Hello, World!";
});

export let source = tg.target(() =>
	tg.directory({
		["Cargo.toml"]: tg.include("./Cargo.toml"),
		["Cargo.lock"]: tg.include("./Cargo.lock"),
		["packages"]: {
			["cli"]: tg.include("./packages/cli"),
			["client"]: tg.include("./packages/client"),
			["error"]: tg.include("./packages/error"),
			["language"]: tg.include("./packages/language"),
			["package"]: tg.include("./packages/package"),
			["runtime"]: tg.include("./packages/runtime"),
			["server"]: tg.include("./packages/server"),
			["vfs"]: tg.include("./packages/vfs"),
		},
	}),
);
