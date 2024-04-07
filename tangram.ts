export default tg.target(async () => {
	return test();
});

export let test = tg.target(async () => {
	return tg.directory({
		"a": "Hello, World!!",
		"b": "Hello, World!",
	});
});
