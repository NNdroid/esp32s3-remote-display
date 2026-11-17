#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
A/B benchmark: software decode vs Amlogic VPU decode (h264_v4l2m2m).

Runs on the S905X3 box, NOT on the dev PC. Both arms decode the SAME live
RTSP stream for the SAME duration and pipe frames through the SAME filters
as rtsp_monitor.py (fps + scale + bgr24), so the only difference is the
decoder. Reports wall time, CPU time (user+sys), CPU load and achieved fps.

Usage:
    python3 bench_hwdec.py                       # 20s per arm, defaults from .env
    python3 bench_hwdec.py --seconds 30          # longer, more stable sample
    python3 bench_hwdec.py --decoder hevc_v4l2m2m  # H.265 camera source
    python3 bench_hwdec.py --skip-hw             # software arm only

Interpretation (see verdict at the end):
    - CPU load drops by >=30% with similar fps  -> worth enabling in .env
    - HW arm fails / fewer frames / higher CPU  -> keep software decode
    - This measures CPU cost, NOT end-to-end latency. Check pipe_wait /
      loop_avg in the monitor log separately for the latency effect.
"""

import argparse
import subprocess
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE = SCRIPT_DIR / ".env"


def load_env(path):
    values = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip()
    return values


def probe(decoder):
    print("[probe] ---- environment ----")
    try:
        uname = subprocess.run(
            ["uname", "-r"], capture_output=True, text=True, timeout=5
        ).stdout.strip()
        print(f"[probe] kernel: {uname}")
    except Exception as exc:
        print(f"[probe] kernel: unavailable ({exc})")

    devices = sorted(str(p) for p in Path("/dev").glob("video*"))
    print(f"[probe] /dev/video*: {', '.join(devices) if devices else 'NONE'}")
    amvenc = sorted(str(p) for p in Path("/dev").glob("amvenc*"))
    print(f"[probe] /dev/amvenc*: {', '.join(amvenc) if amvenc else 'NONE'}")

    fw_dir = Path("/lib/firmware/meson/vdec")
    if fw_dir.is_dir():
        fw = sorted(p.name for p in fw_dir.glob("*.bin"))
        print(f"[probe] vdec firmware: {len(fw)} file(s) - {', '.join(fw[:4])}")
    else:
        print("[probe] vdec firmware: MISSING /lib/firmware/meson/vdec")

    try:
        v4l2 = subprocess.run(
            ["v4l2-ctl", "--list-devices"],
            capture_output=True, text=True, timeout=5,
        ).stdout.strip()
        print(f"[probe] v4l2-ctl: {v4l2 or 'no output'}")
    except FileNotFoundError:
        print("[probe] v4l2-ctl: not installed (optional)")
    except Exception as exc:
        print(f"[probe] v4l2-ctl: error {exc}")

    for kind, flag in (("decoder", "-decoders"), ("encoder", "-encoders")):
        try:
            out = subprocess.run(
                ["ffmpeg", "-hide_banner", flag],
                capture_output=True, text=True, timeout=10,
            ).stdout
            present = decoder in out
            note = "available" if present else "NOT available"
            print(f"[probe] ffmpeg {kind} {decoder}: {note}")
        except Exception as exc:
            print(f"[probe] ffmpeg {kind} check failed: {exc}")

    try:
        dmesg = subprocess.run(
            ["dmesg"], capture_output=True, text=True, timeout=5,
        ).stdout
        lines = [
            line.strip() for line in dmesg.splitlines()
            if "meson-vdec" in line.lower()
        ]
        if lines:
            print(f"[probe] dmesg meson-vdec: {lines[-1]}")
    except Exception:
        pass
    print("[probe] -----------------------\n")


def build_command(rtsp, width, height, fps, seconds, decoder):
    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-loglevel", "error",
        "-rtsp_transport", "tcp",
        "-fflags", "+nobuffer+discardcorrupt+genpts",
        "-max_delay", "0",
        "-flags", "low_delay",
        "-use_wallclock_as_timestamps", "1",
        "-probesize", "50000",
        "-analyzeduration", "100000",
    ]
    if decoder:
        command += ["-c:v", decoder]
    command += [
        "-i", rtsp,
        "-map", "0:v:0",
        "-vf", f"fps={fps},scale={width}:{height}:flags=fast_bilinear",
        "-pix_fmt", "bgr24",
        "-an",
        "-t", str(seconds),
        "-progress", "pipe:1",
        "-nostats",
        "-f", "null",
        "-",
    ]
    return command


def measure_cpu(command, seconds):
    """Return (wall, cpu_user, cpu_sys) using wait4 via resource module."""
    import resource

    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        out, err = proc.communicate(timeout=seconds + 30)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
    wall = time.monotonic() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)

    cpu_user = after.ru_utime - before.ru_utime
    cpu_sys = after.ru_stime - before.ru_stime

    text = out.decode("utf-8", errors="ignore")
    frames = 0
    for line in text.splitlines():
        if line.startswith("frame="):
            try:
                frames = int(line.split("=", 1)[1].strip())
            except ValueError:
                pass
    err_text = err.decode("utf-8", errors="ignore").strip()
    return {
        "rc": proc.returncode,
        "frames": frames,
        "wall": wall,
        "cpu": cpu_user + cpu_sys,
        "cpu_user": cpu_user,
        "cpu_sys": cpu_sys,
        "fps": frames / wall if wall > 0 else 0.0,
        "cpu_load": (cpu_user + cpu_sys) / wall if wall > 0 else 0.0,
        "stderr": err_text[-1200:],
    }


def show(label, r):
    print(
        f"[result] {label}:\n"
        f"         frames={r['frames']}  wall={r['wall']:.1f}s  "
        f"achieved_fps={r['fps']:.2f}\n"
        f"         cpu={r['cpu']:.1f}s (user {r['cpu_user']:.1f} + "
        f"sys {r['cpu_sys']:.1f})  cpu_load={r['cpu_load'] * 100:.0f}%"
    )
    if r["stderr"]:
        print(f"         stderr tail: {r['stderr'][-400:]}")
    print()


def verdict(sw, hw):
    print("[verdict] ---- analysis ----")
    if hw is None:
        print("[verdict] HW arm skipped; no comparison.")
        return
    if sw["frames"] == 0:
        print("[verdict] Software arm produced 0 frames - check RTSP_URL / network.")
        return
    if hw["rc"] != 0 or hw["frames"] == 0:
        print("[verdict] HW decode FAILED. Likely missing firmware / driver / wrong codec.")
        print("[verdict] Keep INPUT_HW_DECODER empty.")
        return

    cpu_saved = sw["cpu"] - hw["cpu"]
    cpu_pct = (cpu_saved / sw["cpu"] * 100) if sw["cpu"] > 0 else 0.0
    frame_pct = (hw["frames"] - sw["frames"]) / sw["frames"] * 100

    print(
        f"[verdict] CPU time: {sw['cpu']:.1f}s -> {hw['cpu']:.1f}s "
        f"({cpu_pct:+.0f}%)\n"
        f"[verdict] frames: {sw['frames']} -> {hw['frames']} "
        f"({frame_pct:+.1f}%)"
    )
    if cpu_pct >= 30 and frame_pct >= -5:
        print(
            "[verdict] PASS: hardware decoding clearly saves CPU without "
            "losing frames.\n"
            "[verdict] Set INPUT_HW_DECODER=<decoder> in .env, then watch "
            "pipe_wait / loop_avg in the monitor log."
        )
    elif cpu_pct > 0 and frame_pct >= -5:
        print(
            "[verdict] MARGINAL: some CPU saved, but under 30%. At 320x240 "
            "software decode is already cheap; leave it off unless the "
            "monitor log shows a real backlog."
        )
    else:
        print(
            "[verdict] NO GAIN: hardware decode is not better here. "
            "Keep software decoding."
        )
    print(
        "[verdict] Remember: this is a CPU benchmark. For latency, compare "
        "pipe_wait / drain / lag_avg / loop_avg in rtsp_monitor.py output."
    )


def main():
    parser = argparse.ArgumentParser(
        description="A/B benchmark software vs VPU decoding on S905X3"
    )
    parser.add_argument("--seconds", type=int, default=20)
    parser.add_argument("--rtsp", default=None)
    parser.add_argument("--width", type=int, default=None)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--fps", type=int, default=None)
    parser.add_argument(
        "--decoder", default="h264_v4l2m2m",
        help="V4L2 M2M decoder to test (h264_v4l2m2m / hevc_v4l2m2m)",
    )
    parser.add_argument("--skip-hw", action="store_true")
    args = parser.parse_args()

    env = load_env(ENV_FILE)
    rtsp = args.rtsp or env.get("RTSP_URL", "")
    width = args.width or int(env.get("WIDTH", "320"))
    height = args.height or int(env.get("HEIGHT", "240"))
    fps = args.fps or int(env.get("INPUT_FPS", env.get("FPS", "24")))

    if not rtsp:
        print("ERROR: RTSP_URL not set. Use --rtsp or set it in .env")
        return 2

    print(
        f"[config] rtsp={rtsp}  size={width}x{height}  "
        f"input_fps={fps}  seconds={args.seconds}\n"
    )
    probe(args.decoder)

    sw_cmd = build_command(rtsp, width, height, fps, args.seconds, None)
    print("[bench] arm 1/2: software decode")
    sw = measure_cpu(sw_cmd, args.seconds)
    show("software decode", sw)

    hw = None
    if not args.skip_hw:
        hw_cmd = build_command(
            rtsp, width, height, fps, args.seconds, args.decoder
        )
        print(f"[bench] arm 2/2: hardware decode ({args.decoder})")
        hw = measure_cpu(hw_cmd, args.seconds)
        show(f"hardware decode ({args.decoder})", hw)

    verdict(sw, hw)
    return 0


if __name__ == "__main__":
    sys.exit(main())
