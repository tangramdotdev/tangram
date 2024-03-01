export default tg.target(async () => {
	return await Promise.all([foo(5), foo(6), foo(7), foo(8), foo(9)]);
});

export let foo = tg.target(async (n: number) => {
	console.log("starting");
	for (let i = 0; i < n; i++) {
		await tg.sleep(1);
		console.log(`print ${i + 1}`);
	}
	return "Hello, World!";
});
