#!/usr/bin/env python3
"""Check authored frames and exercise presenterm in a PTY, without personal config.

This validates emitted image packets, not a physical terminal's image display.
"""

import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import pty
import re
import select
import struct
import subprocess
import termios
import time
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
NS = {"s": "http://www.w3.org/2000/svg"}
COUNTS = [2, 2, 3, 3, 3, 2, 3, 3, 2, 3, 3, 4]
PACKET = re.compile(rb"\x1b\]1337;File=[^\x07]*\x07")


def check_assets():
    manifest = json.loads((ROOT / "slides/assets/manifest.json").read_text())
    assert len(manifest) == sum(COUNTS) == 33
    previous, final_paths = None, []
    for item in manifest:
        source = ROOT / "slides" / item["svg"]
        tree = ET.fromstring(source.read_text())
        assert tree.attrib["viewBox"] == "0 0 1600 900"
        groups = tree.findall("s:g", NS)
        assert len(groups) == ord(item["frame"]) - 64
        if previous and previous["slide"] == item["slide"]:
            old = ET.fromstring((ROOT / "slides" / previous["svg"]).read_text()).findall("s:g", NS)
            assert [ET.tostring(g) for g in groups[:-1]] == [ET.tostring(g) for g in old], item
        data = (ROOT / "slides" / item["png"]).read_bytes()
        assert data[:8] == b"\x89PNG\r\n\x1a\n"
        assert struct.unpack(">II", data[16:24]) == (1600, 900)
        if ord(item["frame"]) - 64 == COUNTS[item["slide"] - 1]:
            final_paths.append(item["png"])
        previous = item
    for file, expected in [("talk.md", [item["png"] for item in manifest]), ("final.md", final_paths)]:
        text = (ROOT / "slides" / file).read_text()
        assert re.findall(r"!\[image:width:100%\]\(([^)]+)\)", text) == expected
        assert text.count("<!-- end_slide -->") == len(expected) - 1
    return {"logical_slides": 12, "live_frames": 33, "static_frames": 12,
            "dimensions": [1600, 900], "unchanged_reveal_groups": True}


def check_terminal(filename, rows, cols, count):
    master, slave = pty.openpty()
    fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack("HHHH", rows, cols, 1600, 900))
    env = dict(os.environ, TERM="xterm-256color")
    env.pop("NO_COLOR", None)
    args = ["presenterm", "--config-file", "slides/presenterm.yaml", "--image-protocol", "iterm2",
            "--validate-overflows", "slides/" + filename]
    proc = subprocess.Popen(args, stdin=slave, stdout=slave, stderr=slave, cwd=ROOT, env=env)
    os.close(slave)
    hashes = []
    started = time.monotonic()

    def receive_image(timeout=30):
        data = b""
        answered = {}
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if select.select([master], [], [], .1)[0]:
                try:
                    data += os.read(master, 1048576)
                except OSError:
                    break
                # A bare PTY has no terminal emulator to answer capability probes.
                # Reply only to observed requests, with the geometry set above.
                replies = {b"\x1b[5n": b"\x1b[0n", b"\x1b[6n": b"\x1b[1;1R", b"\x1b[c": b"\x1b[?62;4c",
                           b"\x1b[14t": b"\x1b[4;900;1600t", b"\x1b[16t": b"\x1b[6;22;13t",
                           b"\x1b]10;?": b"\x1b]10;rgb:f5f5/f7f7/fafa\x1b\\",
                           b"\x1b]11;?": b"\x1b]11;rgb:1010/1515/1d1d\x1b\\"}
                for query, response in replies.items():
                    occurrences = data.count(query)
                    if occurrences > answered.get(query, 0):
                        os.write(master, response * (occurrences - answered.get(query, 0)))
                        answered[query] = occurrences
                if b"Error loading presentation" in data or b"overflow" in data.lower():
                    raise AssertionError(data[-2500:].decode(errors="replace"))
                matches = list(PACKET.finditer(data))
                if matches:
                    encoded = matches[-1][0].split(b":", 1)[1][:-1]
                    payload = base64.b64decode(encoded, validate=True)
                    assert payload[:8] == b"\x89PNG\r\n\x1a\n"
                    return hashlib.sha256(payload).hexdigest()
            if proc.poll() is not None:
                break
        raise AssertionError(f"no image packet: {filename}, exit={proc.poll()}, tail={data[-1800:]!r}")

    def key(value):
        os.write(master, value)
        return receive_image()

    try:
        hashes.append(receive_image())
        for index in range(1, count):
            current = key(b" ")
            assert current != hashes[-1], f"frame did not advance: {index+1}"
            hashes.append(current)
        assert len(set(hashes)) == count
        assert key(b"\x1b[D") == hashes[-2], "left arrow"
        assert key(b"G") == hashes[-1], "last frame"
        assert key(b"gg") == hashes[0], "first frame"
        assert key(b"14G" if count == 33 else b"6G") == hashes[13 if count == 33 else 5], "numeric jump"
        os.write(master, b"q")
        end = time.monotonic() + 10
        while proc.poll() is None and time.monotonic() < end:
            if select.select([master], [], [], .1)[0]:
                try:
                    os.read(master, 1048576)
                except OSError:
                    break
        proc.wait(timeout=5)
        assert proc.returncode == 0
        return {"file": filename, "rows": rows, "columns": cols, "frames": count,
                "protocol": "iterm2", "unique_image_packets": len(set(hashes)),
                "navigation": ["space", "left", "G", "gg", "numberG", "q"],
                "seconds": round(time.monotonic() - started, 3), "exit_code": proc.returncode,
                "frame_hashes": hashes}
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=3)
        os.close(master)


def main():
    report = {"assets": check_assets(), "terminal": [],
              "limits": ["image protocol output checked; physical display not observed",
                         "browser routes verified separately; full spoken rehearsal pending"]}
    for file, rows, cols, count in [("talk.md", 40, 120, 33), ("talk.md", 32, 100, 33),
                                   ("final.md", 40, 120, 12)]:
        result = check_terminal(file, rows, cols, count)
        report["terminal"].append(result)
        print(json.dumps({k: v for k, v in result.items() if k != "frame_hashes"}), flush=True)
    (ROOT / "artifacts").mkdir(exist_ok=True)
    (ROOT / "artifacts/presentation-check.json").write_text(json.dumps(report, ensure_ascii=False, indent=2)+"\n")
    print("presentation checks passed")


if __name__ == "__main__":
    main()
