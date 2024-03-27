export default tg.target(async () => {
	return test();
});

export let test = tg.target(async () => {
	return tg.directory({
		"a": "Hello, World!",
		"b": "Hello, World!",
	});
});

export let emoji = tg.target(async () => {
	let i = 0;
	while(true) {
		console.log(`${i}——👍👌👉👈👍👌——`);
		i++;
		await tg.sleep(0.300);
	}
});
