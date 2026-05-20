#!/usr/bin/env python3

import json
import re
import shutil
import struct
import xml.etree.ElementTree as ET
import zlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SVG = ROOT.parents[1] / "tangram.svg"
APP_ICON = ROOT / "Tangram/AppIcon.icon"
FOREGROUND = APP_ICON / "Assets/foreground.png"
MENUBAR = ROOT / "Tangram/Resources/Assets.xcassets/MenuBarIcon.imageset"


def contains(point, polygon):
	x, y = point
	inside = False
	x1, y1 = polygon[-1]
	for x2, y2 in polygon:
		if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / (y2 - y1) + x1:
			inside = not inside
		x1, y1 = x2, y2
	return inside


def color(hex_color):
	return bytes.fromhex(hex_color) + b"\xff"


def path_points(path):
	tokens = re.findall(r"[MLHVZ]|-?\d+(?:\.\d+)?", path)
	command = None
	points = []
	x = y = 0
	i = 0
	while i < len(tokens):
		if tokens[i].isalpha():
			command = tokens[i]
			i += 1
		if command in ["M", "L"]:
			x = float(tokens[i])
			y = float(tokens[i + 1])
			i += 2
			points.append((x, y))
			command = "L"
		elif command == "H":
			x = float(tokens[i])
			i += 1
			points.append((x, y))
		elif command == "V":
			y = float(tokens[i])
			i += 1
			points.append((x, y))
		elif command == "Z":
			command = None
		else:
			raise ValueError(f"unsupported SVG path: {path}")
	return points


def read_svg():
	root = ET.parse(SVG).getroot()
	_, _, width, height = [float(n) for n in root.attrib["viewBox"].split()]
	pieces = []
	for element in root.iter():
		if element.tag.endswith("path") and element.attrib.get("fill", "none") != "none":
			pieces.append((path_points(element.attrib["d"]), element.attrib["fill"].removeprefix("#")))
	return width, height, pieces


def draw_icon(size, scale, view_width, view_height, pieces, gray=False):
	margin = size * (1 - scale) / 2
	x_unit = size * scale / view_width
	y_unit = size * scale / view_height
	pixels = bytearray(size * size * 4)
	for polygon, hex_color in pieces:
		rgba = color(hex_color)
		if gray:
			luma = round(0.2126 * rgba[0] + 0.7152 * rgba[1] + 0.0722 * rgba[2])
			rgba = bytes([luma, luma, luma, 255])
		mapped = [((margin + x * x_unit), (margin + y * y_unit)) for x, y in polygon]
		for y in range(max(0, int(min(p[1] for p in mapped))), min(size, int(max(p[1] for p in mapped)) + 1)):
			for x in range(max(0, int(min(p[0] for p in mapped))), min(size, int(max(p[0] for p in mapped)) + 1)):
				if contains((x + 0.5, y + 0.5), mapped):
					offset = (y * size + x) * 4
					pixels[offset:offset + 4] = rgba
	return bytes(pixels)


def write_png(path, size, pixels):
	def chunk(kind, data):
		return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data) & 0xffffffff)

	rows = b"".join(b"\x00" + pixels[y * size * 4:(y + 1) * size * 4] for y in range(size))
	path.write_bytes(
		b"\x89PNG\r\n\x1a\n"
		+ chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 6, 0, 0, 0))
		+ chunk(b"IDAT", zlib.compress(rows, 9))
		+ chunk(b"IEND", b"")
	)


def main():
	view_width, view_height, pieces = read_svg()

	shutil.rmtree(APP_ICON, ignore_errors=True)
	FOREGROUND.parent.mkdir(parents=True)
	write_png(FOREGROUND, 1024, draw_icon(1024, 0.60, view_width, view_height, pieces))

	(APP_ICON / "icon.json").write_text(json.dumps({
		"fill": {"solid": "srgb:0.000,0.000,0.000,1.000"},
		"fill-specializations": [{"appearance": "dark", "value": {"solid": "srgb:0.000,0.000,0.000,1.000"}}],
		"groups": [{"layers": [{"image-name": "foreground.png"}], "lighting": "combined", "specular": True}],
	}, indent=2) + "\n")

	MENUBAR.mkdir(parents=True, exist_ok=True)
	write_png(MENUBAR / "menu-bar-icon.png", 18, draw_icon(18, 0.88, view_width, view_height, pieces, gray=True))
	write_png(MENUBAR / "menu-bar-icon@2x.png", 36, draw_icon(36, 0.88, view_width, view_height, pieces, gray=True))


if __name__ == "__main__":
	main()
