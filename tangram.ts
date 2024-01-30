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
	for (let i = 0; i < 10; i++) {
		await tg.sleep(1);
		console.log(`print ${i + 1}`);
	}
	return "Hello, World!";
});

export let g = tg.target(async () => {
	console.log("starting");
	for (let i = 0; i < 10; i++) {
		await tg.sleep(1);
		console.log(`print ${i + 1}`);
	}
	return "Hello, World!";
});

export let h = tg.target(async () => {
	console.log("starting");
	for (let i = 0; i < 10; i++) {
		await tg.sleep(1);
		console.log(`print ${i + 1}`);
	}
	return "Hello, World!";
});

export let logLoop = tg.target(async () => {
	let n = 0;
	while (true) {
		console.log(`doing stuff ${n}...`);
		n++;
		await tg.sleep(1);
	}
	return n;
});