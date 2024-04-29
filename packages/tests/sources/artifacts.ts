export default tg.target(() =>
	tg.directory({
		file: "hello, world!\n",
		symlink: tg.symlink("file"),
		directory: tg.directory(),
	}),
);
