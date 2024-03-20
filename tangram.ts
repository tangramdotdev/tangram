export default tg.target(async () => {
	return test();
});

export let test = tg.target(async () => {
	throw new Error("hi");
	return tg.file("Hello, World!");
});
