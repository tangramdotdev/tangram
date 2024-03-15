export default tg.target(async () => {
	return test();
});

export let test = tg.target(async () => {
	return tg.file("Hello, World!");
});
