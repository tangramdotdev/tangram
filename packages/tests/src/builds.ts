export default tg.target(() =>
	tg.directory({
		helloWorld: helloWorld(),
		// "fanout": fanout(),
	})
);

export let helloWorld = tg.target(
	async () =>
		await tg
			.target("echo 'hello, world!' > $OUTPUT")
			.output()
			.then(tg.File.expect)
);

export let fanout = tg.target(() => {
	let m = 8;
	let children = [];
	for (let n = 0; n < m; n++) {
		let directory = tg.directory({
			[`${n}`]: fanoutInner(`${m}${n}`),
		});
		children.push(directory);
	}
	return tg.directory(...children);
});

export let fanoutInner = tg.target((m: number, n: number) => {
	let children = [];
	for (let i = 0; i < m; i++) {
		let directory = tg.directory({
			[`${i}`]: sleep(`${m}${n}`),
		});
		children.push(directory);
	}
	return tg.directory(...children);
});

export let sleep = tg.target(async (arg: string) => {
	await tg.sleep(1);
	return tg.file(arg);
});
