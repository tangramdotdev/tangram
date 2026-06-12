use ../../../test.nu *

# tg.path.Component.isNormal distinguishes ordinary components from the current, parent, and root components.

let server = spawn

let path = artifact {
	tangram.ts: 'export default () => [tg.path.Component.isNormal("a"), tg.path.Component.isNormal("."), tg.path.Component.isNormal(".."), tg.path.Component.isNormal("/")];'
}

let output = tg build $path
snapshot $output '[true,false,false,false]'
