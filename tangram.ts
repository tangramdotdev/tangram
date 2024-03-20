export default tg.target(async () => {
	return test();
});

export let test = tg.target(async () => {
	for (let i = 0; i < 5; i += 1) {
		console.log(i);
		await tg.sleep(1);
	}
	return tg.file("Hello, World!");
});
