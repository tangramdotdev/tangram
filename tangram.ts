export let metadata = {
	name: "tangram",
	version: "0.0.0",
};

export default tg.target(async () => {
	await f();
	await g();
	await h();
});

export let f = tg.target(async () => {
	console.log("starting");
	await tg.sleep(10);
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
