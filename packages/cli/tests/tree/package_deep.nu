use ../../test.nu *

let server = spawn

let leaf_path = artifact {
	tangram.ts: '// leaf package'
}
tg tag leaf/1.0.0 $leaf_path

let middle_path = artifact {
	tangram.ts: 'import * as leaf from "leaf/^1"'
}
tg tag middle/2.0.0 $middle_path

let root_path = artifact {
	tangram.ts: 'import * as middle from "middle/^2"'
}
tg tag root $root_path

let output = tg tree root --kind=package
snapshot $output '
	root: dir_01st3kec6e36n61n81awfrzjbepe7nmk6smb3hfmmp9w1ts10dfxeg
	├╴leaf/1.0.0: dir_010amcfrw5b59n7arj7s890qbarq3fpc0jmzhpqchjmn1y485m1mm0
	└╴middle/2.0.0: dir_01gv2z0a181v3p50hykym6fexsmhd47q5kkyy7xnbertwycxs3hn90
	  └╴leaf/1.0.0: dir_010amcfrw5b59n7arj7s890qbarq3fpc0jmzhpqchjmn1y485m1mm0
'
