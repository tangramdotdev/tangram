import * as std from "std" with { source: "../../packages/packages/std" };

export default async (host: string) => {
	const version = "2026_10_02.cba3570";
	const source = await std
		.download({
			url: `https://passt.top/passt/snapshot/passt-${version}.tar.xz`,
			checksum:
				"sha256:aa75616cc43925f0bd6c05bcfbbaecd77397c03d7bd23591b34ef535be5dfcdc",
			mode: "extract",
		})
		.then(tg.Directory.expect)
		.then(std.directory.unwrap);
	const toolchain = await std.sdkModule.gnu.toolchain({ host });
	const getconf = std.wrap(toolchain.get("bin/getconf").then(tg.File.expect), {
		host,
		libraryPaths: [tg`${toolchain}/lib`],
	});
	return std.build`
		cp -R ${source}/. work
		chmod -R u+w work
		cd work
		make -j1 pasta VERSION=${version}
		mkdir -p ${tg.output}/bin
		cp passt ${tg.output}/bin/pasta
	`
		.cpu(1)
		.env(
			std.sdk({ host }),
			std.utils.make.build({ host }),
			tg.directory({ "bin/getconf": getconf }),
		)
		.host(std.triple.archAndOs(host))
		.named("pasta")
		.then(tg.Directory.expect);
};
