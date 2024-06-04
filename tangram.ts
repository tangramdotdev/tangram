export default tg.target(async () => {
	console.log("foo");
	await tg.sleep(10);
	console.log("bar");
	await tg.sleep(1);
	console.log("baz");
	await tg.sleep(1);
	return undefined;
});
