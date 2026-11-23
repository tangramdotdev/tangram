# /// script
# requires-python = ">=3.10"
# dependencies = ["dmgbuild==1.6.7"]
# ///

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import dmgbuild


def main(app, output):
    if app.name != "Tangram.app" or not app.is_dir():
        raise ValueError("expected the Tangram.app bundle")
    script = Path(__file__).resolve()
    assets = script.parents[2] / "packages/macos/dmg"
    with tempfile.TemporaryDirectory() as directory:
        # Use a blank label so the note's icon reads as an arrow between the items.
        note = Path(directory) / "\N{BRAILLE PATTERN BLANK}.rtf"
        shutil.copyfile(assets / "install.rtf", note)
        subprocess.run(
            ["swift", script.with_name("dmg-icon.swift"), assets / "arrow.png", note],
            check=True,
        )
        dmgbuild.build_dmg(
            filename=str(output),
            volume_name="Tangram",
            settings={
                "files": [str(app), str(note)],
                "format": "UDZO",
                "hide_extensions": [app.name, note.name],
                "icon_locations": {
                    app.name: (157, 170),
                    note.name: (320, 170),
                    "Applications": (483, 170),
                },
                "icon_size": 128,
                "symlinks": {"Applications": "/Applications"},
                "text_size": 13,
                "window_rect": ((200, 160), (640, 397)),
            },
        )


if __name__ == "__main__":
    main(Path(sys.argv[1]), Path(sys.argv[2]))
