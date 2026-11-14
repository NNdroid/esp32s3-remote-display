#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Low-CPU ARM64 RTSP motion monitor (low-latency live streaming).

Live pipeline:
    RTSP -> FFmpeg -> raw BGR -> Python/OpenCV
          -> one FFmpeg MJPEG encoder
               -> UDP MPEG-TS :8888 (same format as reference command)
               -> rolling 1-second Matroska/MJPEG segments

Audio pipeline:
    RTSP -> FFmpeg -> PCM s16le -> UDP :8889

CDN pipeline (queue-isolated, NO extra RTSP session):
    annotated BGR frame -> 1-slot queue -> dedicated writer thread
        -> H.264 FFmpeg -> HLS playlist + .ts segments
    finished event MP4 -> immutable event file + fixed stream.mp4 alias
    motion snapshot    -> latest.jpg

Motion:
    3-frame difference on 160x120, every 2 frames.
    Output defaults to 320x240 @ 24 FPS
    (FPS = INPUT_FPS = LIVE_OUTPUT_FPS = 24).

Event recording:
    - about 5s before first confirmed motion
    - until 10s after the last confirmed motion
    - rolling Matroska/MJPEG files are copied into a per-event directory while
      the event is running, so RAM usage does not grow with event duration
    - the moment motion is confirmed, an instant JPEG snapshot goes to
      Telegram (SNAPSHOT_NOTIFY), long before the MP4 even exists
    - after the event, FFmpeg encodes the Matroska/MJPEG parts to H.264 MP4
    - MP4 is uploaded to Telegram through SOCKS5

Latency design (LOW_LATENCY=1, the default):
    - The frame queue holds the NEWEST frame only. The reader keeps draining
      FFmpeg's stdout and drops stale frames instead of replaying them late.
    - The output FFmpeg has NO "-re": it never paces the pipe at a nominal
      frame rate, so a temporary slowdown cannot become permanent lag.
    - The output PTS is anchored to the wall clock. Otherwise FFmpeg
      timestamps by frame COUNT and a receiver playing by PTS/PCR slowly
      drifts away from real time, growing its buffer second by second.
      That is ONE possible "after a while it is 10s behind" symptom.
    - LIVE_OUTPUT_FPS caps the actual PUSH rate independently of motion
      detection. A simple FIFO receiver that never drops stale frames can
      also accumulate lag; lower LIVE_OUTPUT_FPS (6 -> 4 -> 3) if the
      receiver turns out to be the bottleneck.
    - stdin is flushed after every frame, muxer/AVIO buffering is disabled,
      and the UDP tee branch flushes each packet immediately.

    *** DIAGNOSTIC BLIND SPOT (read this before blaming the receiver) ***
    The burned-in "SENT HH:MM:SS" is stamped at DRAW time, and "age" /
    "lag_avg" measure ONLY how long the frame waited in the Python queue.
    NEITHER can see a backlog BEFORE the frame reaches Python. Therefore
    "SENT shows the current clock, age is tiny, but the picture is 10s
    old" is completely possible: the frame already arrived stale from the
    RTSP demuxer / decoder / raw pipe, and we stamped a fresh clock on it.
    A receiver-side buffer would make the SENT text ITSELF 10s old, so
    that observation rules the receiver out.
    All the single-slot-queue / -re / PTS / pacing work above optimises
    the DOWNSTREAM half only. loop_avg / loop_max (main-loop wall time,
    including the encoder stdin write+flush) was added to expose a stalled
    sender, and INPUT_STALL_RESTART forces an RTSP reconnect - the only
    reliable way to discard buffered frames upstream of Python.

    Wall-clock PTS also keeps rolling segments / event MP4s on real
    elapsed time even when frames were dropped. Set LOW_LATENCY=0 for
    the old no-drop behaviour, or LIVE_WALLCLOCK_PTS=0 for the old
    frame-count timeline if a receiver dislikes timestamp gaps.

Receiver side matters too. MJPEG-in-TS is muxed as private data, so a
player may probe for seconds before showing anything. Suggested ffplay:
    ffplay -fflags +nobuffer -flags low_delay -framedrop -analyzeduration 0 -probesize 32768 -sync ext udp://0.0.0.0:8888
If the player cannot identify the stream, raise -probesize to 1M.
VLC: set network-caching=0 and clock-jitter=0 for the UDP input.

Diagnostic (LIVE_CLOCK_OVERLAY=1, default): every pushed frame carries a
"SENT HH:MM:SS.mmm age=XXms" outlined text at the bottom (transparent
background). Compare that clock with a
phone clock on the receiving machine:
  - gap ~= 0s    -> sender and network are real-time; nothing to fix here
  - gap grows    -> the receiver is accumulating buffer (player / proxy /
                    HLS repackager), fix it there, not in this script
  - age=XXms is the sender-internal frame age and should stay under ~100ms.

A/B control viewer (UDP_VIDEO_URL_EXTRA): unicast UDP reaches only one host,
so set e.g. UDP_VIDEO_URL_EXTRA=udp://192.168.x.x:8890?pkt_size=1316 to send
the exact same encoded packets to a laptop, then open it with the low-latency
ffplay command above. If that window is real-time while the dedicated
receiver is 10s behind, the delay is 100% inside that receiver.

CDN publishing (PUBLISH_ENABLED=1, the default):
    A single LIVE "stream.mp4" cannot be cached by a CDN. MP4 needs its
    moov atom written at the end, browsers need Range/Content-Length, and
    a chunked fMP4 response is no-store passthrough only. What a CDN CAN
    cache is a live HLS playlist plus short immutable segments, and
    FINISHED event MP4s. So this script produces both:

        PUBLISH_DIR/                     (point nginx / CDN origin here)
          hls/stream.m3u8               live URL, no-cache / short TTL
          hls/stream_<epoch>.ts         immutable segments, long TTL
          events/event_<ts>_<id>.mp4    immutable VOD, cache forever
          stream.mp4                    fixed URL = latest event (no-cache)
          latest.json                   pointer to the unique URL (no-cache)
          latest.jpg                    latest motion snapshot (no-cache)
          index.html                    tiny player for local testing

    Segment names use microsecond-epoch numbering so even a restart
    within the same second never reuses a URL with different bytes
    (that would poison a CDN edge cache). The
    HLS muxer writes playlists/segments to .tmp and renames them, so an
    HTTP request can never read a half-written playlist.

    HLS runs in a SEPARATE FFmpeg process fed from a 1-slot QUEUE, not
    from a second RTSP connection. The main loop only does put_nowait();
    one writer thread owns the encoder stdin, so a slow disk or a stalled
    origin can never backpressure the ESP32 low-latency path. HLS thus
    carries the Python overlay (SENT clock / motion boxes / brightness).
    Set HLS_ENABLED=0 to disable, and PUBLIC_BASE_URL (e.g.
    https://cdn.example.com/cam) to append the public MP4 link to the
    Telegram caption.

Important:
    The live Python path does NOT call cv2.imencode().
    MJPEG encoding is performed once by FFmpeg.
    Each frame crosses the pipe ONCE into a writable buffer and is then
    annotated and forwarded in place - no full-frame copies per frame.

All source/log strings are ASCII-safe.
"""

import io
import os
import gzip
import json
import time
import signal
import queue
import shutil
import tempfile
import threading
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

import cv2
import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# ============================================================
# .env loader
# ============================================================
#
# Loads KEY=VALUE pairs from a ".env" file placed next to this script
# (not from the current working directory) into os.environ, BEFORE any
# os.getenv() call below runs. Real environment variables that are
# already set (e.g. exported by systemd/docker) always take priority
# over the .env file: os.environ.setdefault() never overwrites them.
#
# Supported .env syntax:
#   KEY=value
#   KEY="value with spaces"
#   KEY='value'
#   # a comment line
#   (blank lines are ignored)

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_FILE_PATH = Path(os.getenv("ENV_FILE", str(SCRIPT_DIR / ".env")))


def load_dotenv(path: Path) -> int:
    if not path.is_file():
        return 0

    loaded = 0
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        print(f"warning: could not read env file {path}: {exc}", flush=True)
        return 0

    for line_number, line in enumerate(raw.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        if "=" not in line:
            print(
                f"warning: {path}:{line_number}: ignoring line without '=': {line!r}",
                flush=True,
            )
            continue

        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        os.environ.setdefault(key, value)
        loaded += 1

    return loaded


_loaded_count = load_dotenv(ENV_FILE_PATH)
if _loaded_count:
    print(
        f"[env] loaded {_loaded_count} value(s) from {ENV_FILE_PATH}",
        flush=True,
    )
else:
    print(
        f"[env] no .env file found at {ENV_FILE_PATH}; "
        f"using process environment / built-in defaults",
        flush=True,
    )


# ============================================================
# Configuration
# ============================================================


def getenv_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


RTSP_URL = os.getenv(
    "RTSP_URL",
    "rtsp://192.168.100.2:8554/cam0-outdoor-052c",
)

UDP_VIDEO_URL = os.getenv(
    "UDP_VIDEO_URL",
    "udp://192.168.192.233:8888?pkt_size=1316",
)

# Optional SECOND UDP target for a control viewer.
#
# Unicast UDP reaches only ONE host. Set this to your own machine's IP to
# A/B the exact same encoded stream with a low-latency player, while the
# dedicated receiver keeps getting its own copy:
#   UDP_VIDEO_URL_EXTRA=udp://192.168.x.x:8890?pkt_size=1316
# Empty = disabled (no extra CPU or bandwidth).
UDP_VIDEO_URL_EXTRA = os.getenv("UDP_VIDEO_URL_EXTRA", "").strip()

UDP_AUDIO_URL = os.getenv(
    "UDP_AUDIO_URL",
    "udp://192.168.192.233:8889?pkt_size=1024",
)

WIDTH = int(os.getenv("WIDTH", "320"))
HEIGHT = int(os.getenv("HEIGHT", "240"))
FPS = int(os.getenv("FPS", "24"))

# ------------------------------------------------------------------
# Low-latency live streaming (default ON).
#
# The frame queue holds only the NEWEST frame: the reader keeps draining
# the input FFmpeg's stdout and replaces any unconsumed frame with the
# latest one. Stale frames are dropped instead of being played back late,
# so the UDP stream always shows "now" and a temporary slowdown can never
# turn into permanent lag. The output FFmpeg must not use "-re" either.
#
# Set LOW_LATENCY=0 to restore the old no-drop / backpressure behaviour.
# That keeps event recording on exact wall clock time, but any slowdown
# becomes permanent latency (which is where the ~10s delay came from).
# ------------------------------------------------------------------
LOW_LATENCY = getenv_bool("LOW_LATENCY", True)

# Pending frames in low-latency mode. 1 = true "latest frame only".
LIVE_QUEUE_SIZE = max(1, int(os.getenv("LIVE_QUEUE_SIZE", "1")))

# Low-latency muxer options on the UDP tee branch. Set to 0 if an older
# FFmpeg build rejects one of these child options.
LIVE_MUX_TUNING = getenv_bool("LIVE_MUX_TUNING", True)

# Anchor the output MPEG-TS timeline to the REAL wall clock.
#
# By default the rawvideo demuxer timestamps frame N at N/FPS seconds,
# i.e. by frame COUNT. The actual write rate is never exactly FPS (frames
# get dropped, motion detection varies, RTSP jitters). A receiver that
# plays by PTS/PCR therefore sees a timeline that slowly runs faster or
# slower than real time and its buffer grows second by second - this is
# the "after a while it is 10s behind" symptom. Wall-clock timestamps make
# the PTS gap equal the real gap between frames, so the drift disappears.
# It also makes rolling segments / event MP4s follow real elapsed time
# even when frames were dropped.
LIVE_WALLCLOCK_PTS = getenv_bool("LIVE_WALLCLOCK_PTS", True)

# Diagnostic overlay: stamp every pushed frame with the SENDER wall clock.
# Compare the number shown on the receiver with a phone clock - that gap is
# the true end-to-end lag including the player's own jitter buffer. Set
# LIVE_CLOCK_OVERLAY=0 once the source of the delay has been located.
LIVE_CLOCK_OVERLAY = getenv_bool("LIVE_CLOCK_OVERLAY", True)

# ------------------------------------------------------------------
# Slow / simple receivers (ESP32, MCU, bare UDP FIFO players).
#
# A simple receiver normally QUEUES every packet and never drops stale
# frames. If we push even slightly faster than it can consume them, its
# FIFO grows second by second and the picture falls further behind, then
# settles at the buffer limit (e.g. ~10s). Dropping old frames on the
# SENDER side does NOT help - it never lowers the push rate.
#
# Cap the push rate independently from motion detection: the detector
# still runs at FPS, the receiver only gets LIVE_OUTPUT_FPS frames.
# 0 = no cap. If it still lags, lower this (6 -> 4 -> 3) or lower
# MJPEG_QUALITY / WIDTH to cut the bytes per frame.
#
# Frames can only be emitted on the source frame grid, so a cap BELOW the
# input rate lands on integer-division grid points (24 -> 12 -> 8 -> 6).
# A cap >= INPUT_FPS is treated as UNCAPPED: gating a 24fps input on a
# 24fps grid adds nothing except jitter-induced random skips. The
# 5-second log line "tx=" shows the actual measured push rate.
# ------------------------------------------------------------------
LIVE_OUTPUT_FPS = float(os.getenv("LIVE_OUTPUT_FPS", "24"))

# ------------------------------------------------------------------
# CAPTURE frame rate - the real fix for a live drift that GROWS over time.
#
# FPS is only the nominal timeline rate. INPUT_FPS is what the input
# FFmpeg "fps=" filter actually pushes through the rawvideo pipe. Every
# frame travels as WIDTH*HEIGHT*3 raw BGR bytes: at 320x240, 25fps is
# 5.76 MB/s, and with a 6fps push cap ~76% of those bytes are decoded,
# copied through the OS pipe and read by Python only to be discarded.
#
# Measured with a real-time FFmpeg source (verify_backlog.py): if the
# consumer is even 5ms/frame slower than the producer, the backlog grows
# by exactly 5ms x 25fps = 125ms per second and NEVER drains - reaching
# the user's 30s drift in about 4 minutes. Dropping frames downstream
# cannot fix it, because the bytes must be read before they can be dropped.
# Lowering the producer to 8fps with the same slow consumer keeps drift
# flat (+2ms over 6s) because the pipe gets a 125ms gap between frames.
#
# This backlog is INVISIBLE to the SENT overlay: captured_at is stamped
# when Python finishes reading the bytes, so a 30s-old frame still shows
# age=0ms and a current SENT timestamp drawn on top of the old picture.
#
# Defaults to FPS (no behaviour change). Set INPUT_FPS=12 in .env when
# FPS=25 with a 6fps push cap. 12 is an INTEGER MULTIPLE of the 6fps cap,
# so frames land exactly on the push grid: measured tx goes from 5.00fps
# (25fps source) to 6.00fps (12fps source), pipe bandwidth halves from
# 5.76 to 2.76 MB/s, and drift stays flat. 8fps also kills the backlog but
# drops tx to 4.00fps, so it is not the best choice here.
# Motion detection then runs at INPUT_FPS; set DETECT_EVERY_N_FRAMES=1
# to keep roughly the old detection frequency if sensitivity matters.
# ------------------------------------------------------------------
INPUT_FPS = int(os.getenv("INPUT_FPS", str(FPS)))

# Upstream backlog probe + auto-recovery.
# Healthy: read_exact() blocks for ~1000/INPUT_FPS ms waiting for the next
# frame (pipe empty). Backlog: it returns instantly (pipe never empty).
PIPE_INSTANT_MS = float(os.getenv("PIPE_INSTANT_MS", "5"))
PIPE_BACKLOG_MS = float(os.getenv("PIPE_BACKLOG_MS", "8"))
PIPE_BACKLOG_RESTART = getenv_bool("PIPE_BACKLOG_RESTART", True)

# Queue size for the legacy no-drop mode only. Keep it small anyway:
# 25 queued frames at 12 FPS is already ~2s of permanent backlog.
FRAME_QUEUE_SIZE = int(os.getenv("FRAME_QUEUE_SIZE", "8"))

# RTSP reconnect behavior.
# When the camera/server closes the RTSP connection, the input FFmpeg is
# restarted automatically instead of terminating the whole monitor.
RTSP_RECONNECT_DELAY = float(os.getenv("RTSP_RECONNECT_DELAY", "2.0"))
RTSP_RECONNECT_MAX_DELAY = float(os.getenv("RTSP_RECONNECT_MAX_DELAY", "15.0"))

# Socket-level I/O timeout passed to FFmpeg's RTSP demuxer (microseconds).
# Without this, if the upstream camera/server restarts without cleanly
# closing the TCP connection (very common), FFmpeg can block forever on
# a read() that will never return any data or EOF. The process then never
# exits and the existing reconnect logic (which only triggers on process
# exit) never fires, so the monitor silently freezes until killed by hand.
# Set to "0" or empty to omit the flag entirely (useful if your FFmpeg
# build rejects the "-timeout" option).
RTSP_IO_TIMEOUT_US = os.getenv("RTSP_IO_TIMEOUT_US", "10000000")

# ------------------------------------------------------------------
# Upstream backlog - the part the SENT overlay CANNOT see.
#
# A frame can be 10s old before Python receives a single byte of it:
# RTSP/TCP socket buffers, the demuxer queue and the rawvideo pipe all
# hold frames and replay them late. Nothing downstream can undo that.
# ------------------------------------------------------------------
# "tcp" never loses a packet, but head-of-line blocking makes lag only
# grow on a lossy link. "udp" drops instead of accumulating. Same URL.
RTSP_TRANSPORT = os.getenv("RTSP_TRANSPORT", "tcp").strip().lower()

# ------------------------------------------------------------------
# Optional hardware video DECODER for the input FFmpeg (e.g.
# "h264_v4l2m2m", "hevc_v4l2m2m", "h264_rkmpp", "h264_v4l2").
#
# Software decoding of the camera H.264/H.265 stream is the single
# biggest CPU consumer of the whole pipeline. On SoCs with a V4L2
# m2m / request-API / vendor decode block, naming that decoder here
# offloads most of it from the CPU entirely.
#
# The option is placed BEFORE -i (input decoder selection). Empty =
# software decode (previous behaviour). Check availability first:
#   ffmpeg -hide_banner -decoders | grep -i v4l2m2m
# ------------------------------------------------------------------
INPUT_HW_DECODER = os.getenv("INPUT_HW_DECODER", "").strip()

INPUT_FLUSH_PACKETS = getenv_bool("INPUT_FLUSH_PACKETS", True)

# Main-loop stall watchdog. loop_ms spans frame_queue.get() through the
# encoder stdin write+flush, so a blocked UDP/disk write is visible.
# Above the threshold an RTSP reconnect is forced: it flushes every
# buffer upstream of Python and pulls the picture back to real time.
INPUT_STALL_RESTART = getenv_bool("INPUT_STALL_RESTART", True)
LOOP_STALL_RESTART_MS = float(os.getenv("LOOP_STALL_RESTART_MS", "3000"))
LOOP_STALL_COOLDOWN_S = float(os.getenv("LOOP_STALL_COOLDOWN_S", "20"))

# Application-level watchdog, independent of FFmpeg's own timeout.
# If no new frame bytes have been read from FFmpeg's stdout for this many
# seconds, the input FFmpeg is force-killed and restarted even though its
# process may still appear "alive" (this is the actual stall scenario
# described above). Keep this a bit larger than RTSP_IO_TIMEOUT_US so the
# FFmpeg-level timeout normally gets a chance to fire first.
RTSP_STALL_TIMEOUT = float(os.getenv("RTSP_STALL_TIMEOUT", "20.0"))

# Put timeout for the legacy no-drop mode (LOW_LATENCY=0) only.
# The low-latency reader never blocks on put().
FRAME_QUEUE_PUT_TIMEOUT = float(os.getenv("FRAME_QUEUE_PUT_TIMEOUT", "0.5"))

# Motion processing is deliberately kept cheap enough for 25 FPS:
# detection still runs every N frames, while overlay/brightness run every frame.

# Detection resolution and frequency.
DETECT_WIDTH = int(os.getenv("DETECT_WIDTH", "160"))
DETECT_HEIGHT = int(os.getenv("DETECT_HEIGHT", "120"))
DETECT_EVERY_N_FRAMES = int(os.getenv("DETECT_EVERY_N_FRAMES", "2"))

# 3-frame motion parameters at 160x120.
MOTION_THRESHOLD = 22
MIN_MOTION_AREA = 25
MAX_MOTION_BOXES = 8
MORPH_KERNEL_SIZE = 3
DILATE_ITERATIONS = 1
MOTION_CONFIRM_FRAMES = 2
BOX_MERGE_PADDING = 7

# Event timing.
PRE_EVENT_SECONDS = 5.0
POST_EVENT_SECONDS = 10.0
MAX_EVENT_SECONDS = 120.0

# Rolling MJPEG storage.
SEGMENT_SECONDS = 1.0
SEGMENT_COUNT = 20
ROLLING_SEGMENT_DIR = os.getenv(
    "ROLLING_SEGMENT_DIR",
    "/dev/shm/rtsp_motion_segments",
)
EVENT_SEGMENT_DIR = os.getenv(
    "EVENT_SEGMENT_DIR",
    "/tmp/rtsp_motion_events",
)

# FFmpeg MJPEG encoder.
# Leave as "mjpeg" unless your platform provides a hardware MJPEG encoder.
MJPEG_ENCODER = os.getenv(
    "MJPEG_ENCODER",
    "mjpeg",
)
MJPEG_QUALITY = os.getenv(
    "MJPEG_QUALITY",
    "3",
)

# Event H.264 encoder.
# Examples on different ARM platforms can include h264_v4l2m2m or h264_rkmpp,
# but the exact name depends on the FFmpeg build/SoC.
EVENT_H264_ENCODER = os.getenv(
    "EVENT_H264_ENCODER",
    "libx264",
)
EVENT_VIDEO_BITRATE = os.getenv(
    "EVENT_VIDEO_BITRATE",
    "550k",
)
EVENT_X264_PRESET = os.getenv(
    "EVENT_X264_PRESET",
    "ultrafast",
)

# Telegram.
# Prefer putting the real token / chat_id / socks into a .env file next to
# this script. The defaults below are only for local testing convenience.
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "1361867740:AAFkYzuv2XG8pUsUerOzrQVVNtBfBjn1Xlo",
)
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    "-1002272541977",
)
TELEGRAM_SOCKS5 = os.getenv(
    "TELEGRAM_SOCKS5",
    "socks5h://guest:guest@192.168.100.3:7890",
)


# Snapshots and event MP4s share this queue. Give it a couple of extra
# slots so a slow MP4 upload does not make the instant JPEG get dropped.
TELEGRAM_QUEUE_SIZE = int(os.getenv("TELEGRAM_QUEUE_SIZE", "4"))

# SOCKS5 / Telegram network tuning (the main timeout source)
# Connect timeout needs to be generous for slow/high-latency SOCKS proxies.
TELEGRAM_CONNECT_TIMEOUT = float(os.getenv("TELEGRAM_CONNECT_TIMEOUT", "30"))
TELEGRAM_READ_TIMEOUT = float(os.getenv("TELEGRAM_READ_TIMEOUT", "300"))
TELEGRAM_MAX_RETRIES = int(os.getenv("TELEGRAM_MAX_RETRIES", "3"))
TELEGRAM_RETRY_BACKOFF = float(os.getenv("TELEGRAM_RETRY_BACKOFF", "2.0"))

# ------------------------------------------------------------------
# Instant snapshot notifications.
#
# A motion MP4 only exists after POST_EVENT_SECONDS (10s) plus H.264
# encoding. A JPEG snapshot can go out the moment motion is confirmed.
#   - SNAPSHOT_MIN_INTERVAL throttles back-to-back events (0 = no throttle)
#   - NOTIFY_QUIET_HOURS suppresses SNAPSHOTS only (MP4s are still sent),
#     format "23:00-07:00", crossing midnight is supported
# ------------------------------------------------------------------
SNAPSHOT_NOTIFY = getenv_bool("SNAPSHOT_NOTIFY", True)
SNAPSHOT_JPEG_QUALITY = int(os.getenv("SNAPSHOT_JPEG_QUALITY", "85"))
SNAPSHOT_MIN_INTERVAL = float(os.getenv("SNAPSHOT_MIN_INTERVAL", "30"))
NOTIFY_QUIET_HOURS = os.getenv("NOTIFY_QUIET_HOURS", "").strip()


# ------------------------------------------------------------------
# CDN publishing: local static directory used as the CDN / nginx origin.
#
# A SINGLE LIVE "stream.mp4" is not CDN-cacheable (moov is written at the
# end, browsers need Range/Content-Length, chunked fMP4 = no-store only).
# Two formats ARE cacheable, and this script produces both:
#   1. live HLS  : short immutable .ts segments + a tiny no-cache .m3u8
#   2. event VOD : finished, immutable .mp4 files (+ a fixed no-cache alias)
#
# PUBLISH_DIR is just a directory. Point nginx (or a CDN origin pull) at
# it; no application server is needed. PUBLISH_ENABLED=0 disables ALL
# publishing without touching the live UDP / Telegram paths.
#
# PUBLIC_BASE_URL is the public prefix, e.g. https://cdn.example.com/cam
# It is OPTIONAL: local files still work with a relative URL. When set,
# the public MP4 link is appended to the Telegram caption.
# ------------------------------------------------------------------
PUBLISH_ENABLED = getenv_bool("PUBLISH_ENABLED", True)
PUBLISH_DIR = os.getenv(
    "PUBLISH_DIR",
    str(SCRIPT_DIR / "public"),
)
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")

# Number of immutable event MP4s kept in events/. Old ones are pruned so
# the origin directory cannot grow forever. 0 = keep everything.
PUBLISH_KEEP_EVENTS = max(0, int(os.getenv("PUBLISH_KEEP_EVENTS", "20")))
PUBLISH_LATEST_JPG = getenv_bool("PUBLISH_LATEST_JPG", True)

# ------------------------------------------------------------------
# Live HLS output - ONE RTSP session, queue-isolated writer.
#
# HLS is fed by the SAME annotated BGR frames the main loop already has.
# The main loop only does a non-blocking put_nowait() into a 1-slot queue
# (drop stale on full). A dedicated thread owns the H.264 -> HLS FFmpeg
# process and its stdin. If the disk / origin stalls, ONLY that thread
# blocks: the raw BGR pipe feeding UDP/ESP32 is untouched, so the
# "grows to 10s" lag cannot come back.
#
# No second RTSP connection, no second decode/scale. HLS therefore DOES
# carry the Python overlay (SENT clock, motion boxes, brightness).
#
# -use_wallclock_as_timestamps 1 is MANDATORY here. Without it the
# rawvideo demuxer timestamps by frame COUNT and dropped frames make the
# HLS timeline drift (measured: 6.5s of media over 11s of wall clock).
# Offline proof: 3s and 8s zero-frame stalls still give uniform 2s
# segments, zero DISCONTINUITY tags, zero ffmpeg warnings.
# ------------------------------------------------------------------
HLS_ENABLED = getenv_bool("HLS_ENABLED", True)
# -framerate MUST match the rate at which HlsPublisher actually writes
# frames (the main loop's push cap). FFmpeg runs in CFR mode: declaring 25
# while feeding 6 makes it DUPLICATE every frame to fill the declared rate.
# Measured locally (diag_hls.py): 6fps feed + "-framerate 25" -> 50 packets
# per 2s segment at 25/1 fps; "-framerate 6" + "-g 12" -> 12 packets per
# 2s segment at 6/1 fps, clean EXTINF:2.000000. The live ARM box showed the
# same effect more strongly: 99 packets / 3s at 33/1 fps with EXTINF:3.
# -g itself need not match: the HLS muxer forces an IDR at each segment
# boundary on its own (verify_gop.py). Only the declared -framerate matters.
# An explicit HLS_FPS in .env still overrides this.
_hls_feed_fps = LIVE_OUTPUT_FPS if LIVE_OUTPUT_FPS > 0 else INPUT_FPS
HLS_FPS = int(os.getenv("HLS_FPS", str(max(1, round(_hls_feed_fps)))))
HLS_QUEUE_SIZE = max(1, int(os.getenv("HLS_QUEUE_SIZE", "1")))
HLS_TIME = float(os.getenv("HLS_TIME", "2"))
# Number of segments kept in the live playlist AND on disk. FFmpeg's
# delete_segments removes a .ts as soon as it falls out of this window, so
# 5 x 2s = only ~10s of origin lifetime: a CDN edge or a player holding a
# playlist for a few seconds gets a 404 on every segment. 15 x 2s = ~30s,
# which is still small (a few MB) but survives CDN cache lag. Raise further
# (30+) if your CDN has slow edge propagation.
HLS_LIST_SIZE = int(os.getenv("HLS_LIST_SIZE", "15"))
HLS_H264_ENCODER = os.getenv("HLS_H264_ENCODER", "libx264")
HLS_VIDEO_BITRATE = os.getenv("HLS_VIDEO_BITRATE", "700k")
HLS_X264_PRESET = os.getenv("HLS_X264_PRESET", "ultrafast")
HLS_RESTART_DELAY = float(os.getenv("HLS_RESTART_DELAY", "2.0"))
HLS_RESTART_MAX_DELAY = float(os.getenv("HLS_RESTART_MAX_DELAY", "30.0"))

DISPLAY = getenv_bool("DISPLAY", False)
DEBUG = getenv_bool("DEBUG", True)


# ============================================================
# Global stop
# ============================================================

STOP_EVENT = threading.Event()


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def signal_handler(signum, frame):
    STOP_EVENT.set()
    log("received stop signal")


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


# ============================================================
# Telegram (SOCKS5-optimised)
# ============================================================

def _mask_proxy_url(url: str) -> str:
    """Hide credentials when logging a proxy URL."""
    if not url:
        return ""
    try:
        p = urlparse(url)
        if p.username or p.password:
            netloc = p.hostname or ""
            if p.port:
                netloc = f"{netloc}:{p.port}"
            return f"{p.scheme}://***:***@{netloc}"
        return url
    except Exception:
        return "***"


class TelegramClient:
    def __init__(self, bot_token: str, chat_id: str, socks5_url: str):
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.socks5_url = socks5_url.strip()
        self.session = requests.Session()
        self.session.trust_env = False  # never pick up system HTTP_PROXY

        # Robust retry for transient network / proxy failures
        retry = Retry(
            total=TELEGRAM_MAX_RETRIES,
            connect=TELEGRAM_MAX_RETRIES,
            read=TELEGRAM_MAX_RETRIES,
            backoff_factor=TELEGRAM_RETRY_BACKOFF,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        if self.socks5_url:
            # socks5h = resolve hostname through the proxy (recommended)
            self.session.proxies.update({
                "http": self.socks5_url,
                "https": self.socks5_url,
            })
            log(f"telegram socks5 configured: {_mask_proxy_url(self.socks5_url)}")
        else:
            log("warning: telegram socks5 is not configured")

        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"
        self._timeout = (TELEGRAM_CONNECT_TIMEOUT, TELEGRAM_READ_TIMEOUT)

    def check_socks_support(self) -> None:
        """Verify that requests can use SOCKS5 (needs PySocks)."""
        try:
            import socks  # noqa: F401
            version = getattr(socks, "__version__", "unknown")
            log(f"PySocks loaded: {version}")
        except ImportError:
            raise RuntimeError(
                "SOCKS5 support unavailable. "
                "Install with: pip install 'requests[socks]' PySocks"
            )

    def test_proxy(self, timeout: Optional[float] = None) -> bool:
        """
        Test SOCKS5 reachability to Telegram.
        Uses a short connect timeout first, then a full API call.
        """
        if not self.socks5_url:
            log("telegram proxy test skipped (no socks configured)")
            return True

        connect_to = timeout or min(20.0, TELEGRAM_CONNECT_TIMEOUT)
        try:
            # 1) Cheap connectivity check (just reach the API host)
            r = self.session.get(
                "https://api.telegram.org",
                timeout=(connect_to, 15),
            )
            log(f"telegram proxy basic test: HTTP {r.status_code}")

            # 2) Real bot API call (getMe) - proves token + proxy both work
            if self.bot_token:
                me = self.request("GET", "getMe", timeout=(connect_to, 20))
                if me and me.get("ok"):
                    username = me.get("result", {}).get("username", "?")
                    log(f"telegram bot ok: @{username}")
                    return True
                log("telegram getMe failed (token or network)")
                return False
            return True

        except requests.exceptions.ConnectTimeout:
            log(
                f"telegram proxy CONNECT TIMEOUT after {connect_to}s - "
                f"proxy may be down, slow, or blocked"
            )
            return False
        except requests.exceptions.ProxyError as exc:
            log(f"telegram proxy error: {exc}")
            return False
        except requests.exceptions.SSLError as exc:
            log(f"telegram SSL error through proxy: {exc}")
            return False
        except Exception as exc:
            log(
                f"telegram proxy test failed: "
                f"{type(exc).__name__}: {exc}"
            )
            return False

    def request(
        self,
        method: str,
        endpoint: str,
        timeout: Optional[tuple] = None,
        **kwargs,
    ) -> Optional[dict]:
        if not self.bot_token or not self.chat_id:
            return None

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        to = timeout or self._timeout

        try:
            response = self.session.request(
                method=method,
                url=url,
                timeout=to,
                **kwargs,
            )
            response.raise_for_status()
            result = response.json()
            if not result.get("ok"):
                log(f"telegram api error: {result}")
                return None
            return result
        except requests.exceptions.ConnectTimeout:
            log(f"telegram connect timeout ({to[0]}s) for {endpoint}")
        except requests.exceptions.ReadTimeout:
            log(f"telegram read timeout ({to[1]}s) for {endpoint}")
        except requests.exceptions.ProxyError as exc:
            log(f"telegram proxy error: {exc}")
        except requests.RequestException as exc:
            log(f"telegram network error: {exc}")
        except Exception as exc:
            log(f"telegram request error: {type(exc).__name__}: {exc}")
        return None

    def send_video(self, video_path: str, caption: str) -> bool:
        if not os.path.isfile(video_path):
            log(f"telegram video not found: {video_path}")
            return False

        size_mb = os.path.getsize(video_path) / (1024 * 1024)
        log(f"telegram upload start: {size_mb:.2f} MB")

        # Manual retry loop on top of urllib3 Retry for large file uploads
        last_error = None
        for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
            try:
                with open(video_path, "rb") as fp:
                    result = self.request(
                        "POST",
                        "sendVideo",
                        data={
                            "chat_id": self.chat_id,
                            "caption": caption,
                            "supports_streaming": "true",
                        },
                        files={
                            "video": (
                                os.path.basename(video_path),
                                fp,
                                "video/mp4",
                            )
                        },
                        timeout=self._timeout,
                    )

                if result is not None:
                    log(f"telegram upload success (attempt {attempt})")
                    return True

                last_error = "api returned not-ok"
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log(f"telegram upload attempt {attempt} failed: {last_error}")

            if attempt < TELEGRAM_MAX_RETRIES:
                sleep_s = TELEGRAM_RETRY_BACKOFF * (2 ** (attempt - 1))
                log(f"telegram retry in {sleep_s:.1f}s ...")
                if STOP_EVENT.wait(sleep_s):
                    break

        log(f"telegram upload failed after {TELEGRAM_MAX_RETRIES} attempts: {last_error}")
        return False

    def send_photo(self, photo_path: str, caption: str) -> bool:
        """Send a small JPEG snapshot using the same retry policy."""
        if not os.path.isfile(photo_path):
            log(f"telegram snapshot not found: {photo_path}")
            return False

        last_error = None
        for attempt in range(1, TELEGRAM_MAX_RETRIES + 1):
            try:
                with open(photo_path, "rb") as fp:
                    result = self.request(
                        "POST",
                        "sendPhoto",
                        data={
                            "chat_id": self.chat_id,
                            "caption": caption,
                        },
                        files={
                            "photo": (
                                os.path.basename(photo_path),
                                fp,
                                "image/jpeg",
                            )
                        },
                        timeout=self._timeout,
                    )
                if result is not None:
                    log(f"telegram snapshot sent (attempt {attempt})")
                    return True
                last_error = "api returned not-ok"
            except Exception as exc:
                last_error = f"{type(exc).__name__}: {exc}"
                log(
                    f"telegram snapshot attempt {attempt} failed: "
                    f"{last_error}"
                )

            if attempt < TELEGRAM_MAX_RETRIES:
                sleep_s = TELEGRAM_RETRY_BACKOFF * (2 ** (attempt - 1))
                if STOP_EVENT.wait(sleep_s):
                    break

        log(
            f"telegram snapshot failed after "
            f"{TELEGRAM_MAX_RETRIES} attempts: {last_error}"
        )
        return False


class TelegramWorker:
    def __init__(self, client: TelegramClient):
        self.client = client
        self.jobs = queue.Queue(maxsize=TELEGRAM_QUEUE_SIZE)
        self.thread = threading.Thread(
            target=self._run,
            name="telegram-worker",
            daemon=True,
        )
        self.thread.start()

    def _submit(self, kind: str, path: str, caption: str) -> bool:
        try:
            self.jobs.put_nowait((kind, path, caption))
            return True
        except queue.Full:
            log(f"telegram queue full; dropping {kind} upload")
            return False

    def submit(self, video_path: str, caption: str) -> bool:
        """Existing MP4 event path - keep the 2-argument signature."""
        return self._submit("video", video_path, caption)

    def submit_photo(self, photo_path: str, caption: str) -> bool:
        return self._submit("photo", photo_path, caption)

    def _run(self):
        while not STOP_EVENT.is_set() or not self.jobs.empty():
            try:
                kind, path, caption = self.jobs.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                if kind == "photo":
                    self.client.send_photo(path, caption)
                else:
                    self.client.send_video(path, caption)
            finally:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    log(f"failed to remove {kind} file: {exc}")
                self.jobs.task_done()


def _silent_remove(path: str) -> None:
    try:
        os.remove(path)
    except OSError:
        pass


def in_quiet_hours(now=None) -> bool:
    """
    True when NOTIFY_QUIET_HOURS (e.g. "23:00-07:00") covers `now`.
    Ranges crossing midnight are supported; malformed text is ignored.
    """
    if not NOTIFY_QUIET_HOURS:
        return False
    try:
        start_s, end_s = NOTIFY_QUIET_HOURS.split("-", 1)

        def to_minutes(text):
            hh, mm = text.strip().split(":")
            return (int(hh) * 60 + int(mm)) % 1440

        start, end = to_minutes(start_s), to_minutes(end_s)
    except Exception:
        return False

    if now is None:
        now = datetime.now()
    cur = now.hour * 60 + now.minute
    if start <= end:
        return start <= cur < end
    return cur >= start or cur < end


class SnapshotNotifier:
    """
    Send an instant JPEG the moment a motion event is confirmed, long
    before the MP4 exists (that one needs POST_EVENT_SECONDS + H.264).

    This is the ONE place that calls cv2.imencode(): it runs once per
    event, not once per frame, so the live path is unaffected. The JPEG
    goes into the existing Telegram worker queue, so the main loop never
    waits on the network.
    """

    def __init__(
        self,
        worker: TelegramWorker,
        publisher: Optional["MediaPublisher"] = None,
    ):
        self.worker = worker
        self.publisher = publisher
        self.last_sent = 0.0

    def on_event_start(
        self,
        frame,
        event_id: int,
        brightness: float,
        box_count: int,
    ) -> bool:
        if not SNAPSHOT_NOTIFY:
            return False

        wall_now = datetime.now()
        if in_quiet_hours(wall_now):
            log("snapshot skipped: quiet hours")
            return False

        mono_now = time.monotonic()
        if mono_now - self.last_sent < SNAPSHOT_MIN_INTERVAL:
            log("snapshot skipped: throttled")
            return False

        ok, encoded = cv2.imencode(
            ".jpg",
            frame,
            [int(cv2.IMWRITE_JPEG_QUALITY), SNAPSHOT_JPEG_QUALITY],
        )
        if not ok:
            log("snapshot jpeg encode failed")
            return False

        jpeg_bytes = encoded.tobytes()

        # Reuse the SAME encoded JPEG as a static CDN URL. Local file
        # write only - no second JPEG encode, no network in the main loop.
        if self.publisher is not None:
            self.publisher.publish_snapshot(jpeg_bytes)

        fd, path = tempfile.mkstemp(
            prefix="rtsp_snap_",
            suffix=".jpg",
        )
        try:
            with os.fdopen(fd, "wb") as fp:
                fp.write(jpeg_bytes)
        except OSError as exc:
            log(f"snapshot write failed: {exc}")
            _silent_remove(path)
            return False

        caption = (
            f"MOTION {wall_now.strftime('%Y-%m-%d %H:%M:%S')} "
            f"event={event_id} boxes={box_count} "
            f"brightness={brightness:.0f}"
        )
        if not self.worker.submit_photo(path, caption):
            _silent_remove(path)
            return False

        self.last_sent = mono_now
        log(f"snapshot queued: event={event_id}")
        return True


# ============================================================
# FFmpeg helpers
# ============================================================

def ensure_dir(path_text: str) -> Path:
    path = Path(path_text)
    try:
        path.mkdir(parents=True, exist_ok=True)
        return path
    except OSError:
        fallback = Path(tempfile.gettempdir()) / path.name
        fallback.mkdir(parents=True, exist_ok=True)
        log(f"directory fallback: {fallback}")
        return fallback


def cleanup_rolling_dir(path: Path) -> None:
    for item in path.glob("seg_*.mkv"):
        try:
            item.unlink()
        except OSError:
            pass


# ============================================================
# CDN publishing: static files for nginx / CDN origin pull
# ============================================================
#
# Every file is written to a ".tmp" sibling and then os.replace()d into
# place. A CDN / nginx request therefore never sees a half-written MP4,
# playlist, JSON pointer or JPEG. This matters a lot for Range requests.

def _gzip_bytes(data: bytes) -> bytes:
    """
    Deterministic gzip (mtime=0) for nginx "gzip_static on".

    A fixed mtime keeps the output byte-identical across restarts, so the
    publish step can compare sizes instead of rewriting the file (and
    invalidating the CDN edge copy) on every launch.
    """
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb", compresslevel=9, mtime=0) as fh:
        fh.write(data)
    return buf.getvalue()


def _atomic_write_bytes(dst: Path, data: bytes) -> None:
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        with tmp.open("wb") as fp:
            fp.write(data)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp, dst)
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


def _atomic_copy_file(src: str, dst: Path) -> None:
    tmp = dst.with_name(dst.name + ".tmp")
    try:
        shutil.copyfile(src, tmp)
        os.replace(tmp, dst)
    finally:
        try:
            tmp.unlink()
        except OSError:
            pass


PLAYER_MARKER = "hls-debug-player-v7"

PLAYER_PAGE = r"""<!doctype html>
<html lang="en" data-theme="dark">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RTSP Monitor - Live Console</title>
<style>
:root{
  --bg:#060a13; --ink:#e9effc; --muted:#8794b0; --dim:#5d6b8a;
  --panel:rgba(17,25,43,.74); --panel2:rgba(11,18,32,.90);
  --line:rgba(125,165,255,.15); --line2:rgba(125,165,255,.28);
  --accent:#628cff; --accent2:#26d3ee; --good:#3ce6a2;
  --warn:#ffc857; --bad:#ff6070; --glow:rgba(98,140,255,.34);
  --shadow:0 18px 46px rgba(0,0,0,.46);
}
html[data-theme="light"]{
  --bg:#eaf0fb; --ink:#16203a; --muted:#5c6c8c; --dim:#8492ac;
  --panel:rgba(255,255,255,.80); --panel2:rgba(255,255,255,.94);
  --line:rgba(35,70,140,.14); --line2:rgba(35,70,140,.26);
  --accent:#315fe0; --accent2:#0a9fbe; --good:#0d9b68;
  --warn:#b8790a; --bad:#d8394a; --glow:rgba(49,95,224,.20);
  --shadow:0 18px 46px rgba(30,55,110,.16);
}
*{box-sizing:border-box}
html,body{min-height:100%}
body{
  margin:0;background:var(--bg);color:var(--ink);
  font:14px/1.55 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;
  transition:background .35s,color .35s;overflow-x:hidden;
}
/* ---------- animated ambient background ---------- */
.aurora{position:fixed;inset:-25%;z-index:-2;pointer-events:none;filter:blur(70px);opacity:.85;transition:opacity .4s}
html[data-theme="light"] .aurora{opacity:.34}
.aurora i{position:absolute;display:block;border-radius:50%;mix-blend-mode:screen}
.aurora i:nth-child(1){width:48vw;height:48vw;left:2%;top:0;background:radial-gradient(circle,rgba(70,105,255,.58),transparent 70%);animation:drift1 24s ease-in-out infinite alternate}
.aurora i:nth-child(2){width:42vw;height:42vw;right:0;top:22%;background:radial-gradient(circle,rgba(20,210,225,.42),transparent 70%);animation:drift2 29s ease-in-out infinite alternate}
.aurora i:nth-child(3){width:36vw;height:36vw;left:34%;bottom:-6%;background:radial-gradient(circle,rgba(150,80,255,.34),transparent 70%);animation:drift3 33s ease-in-out infinite alternate}
@keyframes drift1{to{transform:translate(14vw,9vh) scale(1.2)}}
@keyframes drift2{to{transform:translate(-13vw,-8vh) scale(.85)}}
@keyframes drift3{to{transform:translate(9vw,-12vh) scale(1.15)}}
.grid{position:fixed;inset:0;z-index:-1;pointer-events:none;
  background-image:linear-gradient(rgba(255,255,255,.032) 1px,transparent 1px),linear-gradient(90deg,rgba(255,255,255,.032) 1px,transparent 1px);
  background-size:46px 46px;
  -webkit-mask-image:linear-gradient(to bottom,rgba(0,0,0,.95),transparent 88%);
  mask-image:linear-gradient(to bottom,rgba(0,0,0,.95),transparent 88%)}
html[data-theme="light"] .grid{background-image:linear-gradient(rgba(30,70,140,.055) 1px,transparent 1px),linear-gradient(90deg,rgba(30,70,140,.055) 1px,transparent 1px)}
/* ---------- shell ---------- */
.wrap{max-width:1300px;margin:0 auto;padding:26px 22px 40px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:18px;
  box-shadow:var(--shadow);-webkit-backdrop-filter:blur(14px);backdrop-filter:blur(14px);
  position:relative}
.panel::before{content:'';position:absolute;left:18px;right:18px;top:0;height:1px;pointer-events:none;
  background:linear-gradient(90deg,transparent,rgba(125,165,255,.42),transparent)}
.top{display:flex;align-items:center;gap:16px;padding:14px 20px;margin-bottom:18px;flex-wrap:wrap;
  animation:rise .6s both}
.brand{display:flex;align-items:center;gap:12px;margin-right:auto}
.logo{width:40px;height:40px;border-radius:12px;position:relative;flex:none;
  background:linear-gradient(145deg,var(--accent),var(--accent2));box-shadow:0 6px 22px var(--glow)}
.logo::before{content:'';position:absolute;inset:9px;border:2px solid rgba(255,255,255,.92);border-radius:50%}
.logo::after{content:'';position:absolute;left:50%;top:50%;width:6px;height:6px;margin:-3px;border-radius:50%;background:#fff;animation:blip 2.2s infinite}
@keyframes blip{0%,100%{box-shadow:0 0 0 0 rgba(255,255,255,.7)}60%{box-shadow:0 0 0 9px rgba(255,255,255,0)}}
.brand h1{margin:0;font-size:17px;letter-spacing:.02em;font-weight:650}
.brand p{margin:1px 0 0;font-size:11px;color:var(--muted);letter-spacing:.12em;text-transform:uppercase}
.live-badge{display:inline-flex;align-items:center;gap:8px;padding:6px 12px;border-radius:999px;
  border:1px solid rgba(255,96,112,.4);background:rgba(255,96,112,.10);color:var(--bad);
  font:700 11px/1 ui-monospace,monospace;letter-spacing:.16em}
.dot{width:8px;height:8px;border-radius:50%;background:var(--bad);animation:pulse 1.8s infinite}
@keyframes pulse{0%{box-shadow:0 0 0 0 rgba(255,96,112,.6)}70%{box-shadow:0 0 0 11px rgba(255,96,112,0)}100%{box-shadow:0 0 0 0 rgba(255,96,112,0)}}
.clock{font:600 13px/1 ui-monospace,monospace;color:var(--muted);letter-spacing:.08em;min-width:76px;text-align:right}
.btn{border:1px solid var(--line);background:var(--panel2);color:var(--ink);border-radius:10px;
  padding:8px 12px;font:600 11px/1 system-ui,sans-serif;letter-spacing:.05em;cursor:pointer;
  transition:transform .18s,border-color .18s,box-shadow .18s,background .18s;white-space:nowrap}
.btn:hover{transform:translateY(-2px);border-color:var(--accent);box-shadow:0 7px 20px var(--glow)}
.btn:active{transform:translateY(0)}
.tools{display:flex;gap:8px;flex-wrap:wrap}
/* ---------- layout ---------- */
.main{display:grid;grid-template-columns:minmax(0,1fr) 316px;gap:18px;align-items:start}
@media(max-width:940px){.main{grid-template-columns:1fr}}
.card{padding:16px 18px 18px}
.card + .card{margin-top:18px}
.hd{display:flex;align-items:center;gap:10px;margin-bottom:14px}
.hd h2{margin:0;font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted);font-weight:700}
.hd .sp{flex:1}
.chip{padding:4px 10px;border-radius:999px;font:700 10px/1 ui-monospace,monospace;letter-spacing:.1em;
  border:1px solid var(--line2);background:rgba(125,165,255,.10);color:var(--muted);transition:.25s}
.chip.good{color:var(--good);border-color:rgba(60,230,162,.42);background:rgba(60,230,162,.12)}
.chip.bad{color:var(--bad);border-color:rgba(255,96,112,.42);background:rgba(255,96,112,.12)}
.chip.warn{color:var(--warn);border-color:rgba(255,200,87,.42);background:rgba(255,200,87,.12)}
/* ---------- video screen ---------- */
.screen{position:relative;aspect-ratio:16/9;background:#04070d;border-radius:14px;overflow:hidden;
  border:1px solid var(--line2);box-shadow:inset 0 0 60px rgba(0,0,0,.7),0 0 0 1px rgba(0,0,0,.3)}
.screen video{width:100%;height:100%;object-fit:contain;display:block;background:#04070d}
.corner{position:absolute;width:24px;height:24px;pointer-events:none;z-index:3;opacity:.85;
  border-color:var(--accent2);border-style:solid;border-width:0;filter:drop-shadow(0 0 5px var(--accent2))}
.corner.tl{left:12px;top:12px;border-left-width:2px;border-top-width:2px}
.corner.tr{right:12px;top:12px;border-right-width:2px;border-top-width:2px}
.corner.bl{left:12px;bottom:12px;border-left-width:2px;border-bottom-width:2px}
.corner.br{right:12px;bottom:12px;border-right-width:2px;border-bottom-width:2px}
.scan{position:absolute;left:0;right:0;top:-14%;height:14%;pointer-events:none;z-index:2;
  background:linear-gradient(to bottom,transparent,rgba(90,190,255,.13),transparent);animation:scan 7s linear infinite}
@keyframes scan{to{top:112%}}
.hud{position:absolute;left:0;right:0;top:0;z-index:3;display:flex;align-items:center;gap:10px;
  padding:14px 18px;font:700 11px/1 ui-monospace,monospace;letter-spacing:.12em;
  text-shadow:0 1px 7px rgba(0,0,0,.95);
  background:linear-gradient(to bottom,rgba(0,0,0,.68),transparent);pointer-events:none}
.hud .tag{display:inline-flex;align-items:center;gap:7px;color:#fff}
.hud .tag i{width:7px;height:7px;border-radius:50%;background:var(--bad);animation:pulse 1.8s infinite}
.hud .r{margin-left:auto;color:rgba(255,255,255,.82)}
.hud .rec{color:#fff;opacity:.8}
.spinner{position:absolute;inset:0;z-index:4;display:grid;place-items:center;pointer-events:none;
  background:rgba(4,7,13,.55);transition:opacity .5s}
.spinner.hide{opacity:0}
.spinner b{width:46px;height:46px;border-radius:50%;border:2px solid rgba(255,255,255,.16);
  border-top-color:var(--accent2);animation:spin 1s linear infinite}
.spinner span{position:absolute;margin-top:82px;font:600 10px/1 ui-monospace,monospace;letter-spacing:.18em;color:rgba(255,255,255,.7)}
@keyframes spin{to{transform:rotate(360deg)}}
.screen.flash{animation:flash .5s ease-out}
@keyframes flash{0%{box-shadow:0 0 0 0 rgba(60,230,162,.55),0 0 44px rgba(60,230,162,.55),inset 0 0 60px rgba(0,0,0,.7)}
                 100%{box-shadow:0 0 0 14px rgba(60,230,162,0),0 0 0 rgba(60,230,162,0),inset 0 0 60px rgba(0,0,0,.7)}}
.under{display:flex;align-items:center;gap:12px;margin-top:12px;flex-wrap:wrap;font-size:11px;color:var(--muted)}
.under a{color:var(--accent2);text-decoration:none;border-bottom:1px dashed currentColor}
.under a:hover{color:var(--accent)}
.bar{position:relative;height:6px;border-radius:99px;background:rgba(125,165,255,.13);overflow:hidden;flex:1;min-width:120px}
.bar>i{display:block;height:100%;width:0%;border-radius:99px;position:relative;overflow:hidden;
  background:linear-gradient(90deg,var(--accent),var(--accent2));transition:width .6s ease}
.bar>i::after{content:'';position:absolute;inset:0;
  background:linear-gradient(90deg,transparent,rgba(255,255,255,.5),transparent);animation:shimmer 1.7s linear infinite}
@keyframes shimmer{from{transform:translateX(-100%)}to{transform:translateX(100%)}}
/* ---------- telemetry ---------- */
.stats{display:grid;grid-template-columns:1fr 1fr;gap:10px}
.stat{padding:12px 13px;border:1px solid var(--line);border-radius:13px;background:var(--panel2);
  position:relative;overflow:hidden;animation:rise .6s both;
  transition:transform .2s,border-color .2s,box-shadow .2s}
.stat:hover{transform:translateY(-3px);border-color:var(--line2);box-shadow:0 10px 26px var(--glow)}
.stat:nth-child(1){animation-delay:.05s}.stat:nth-child(2){animation-delay:.12s}
.stat:nth-child(3){animation-delay:.19s}.stat:nth-child(4){animation-delay:.26s}
.stat::before{content:'';position:absolute;left:0;top:0;bottom:0;width:2px;background:linear-gradient(var(--accent),var(--accent2))}
.stat .k{font:700 9px/1 ui-monospace,monospace;letter-spacing:.14em;color:var(--dim);text-transform:uppercase}
.stat .v{margin-top:7px;font:700 20px/1 ui-monospace,monospace;letter-spacing:-.02em;color:var(--ink);
  font-variant-numeric:tabular-nums}
.stat .v small{font-size:11px;font-weight:500;color:var(--muted);margin-left:3px}
.heart{display:flex;align-items:flex-end;gap:3px;height:26px;margin-top:14px}
.heart i{flex:1;height:35%;border-radius:2px;background:linear-gradient(var(--accent),var(--accent2));
  opacity:.8;animation:beat 1.15s ease-in-out infinite}
.heart i:nth-child(2){animation-delay:.10s}.heart i:nth-child(3){animation-delay:.20s}
.heart i:nth-child(4){animation-delay:.30s}.heart i:nth-child(5){animation-delay:.40s}
.heart i:nth-child(6){animation-delay:.50s}.heart i:nth-child(7){animation-delay:.60s}
.heart i:nth-child(8){animation-delay:.70s}.heart i:nth-child(9){animation-delay:.80s}
.heart i:nth-child(10){animation-delay:.90s}.heart i:nth-child(11){animation-delay:1.00s}
.heart i:nth-child(12){animation-delay:1.10s}
.heart.off i{background:var(--dim);opacity:.28;animation:none;height:18%}
.heart.bad i{background:var(--bad)}
@keyframes beat{0%,100%{height:22%}50%{height:100%}}
.mini-links{display:flex;flex-wrap:wrap;gap:7px;margin-top:12px}
.mini-links a{font:600 10px/1 ui-monospace,monospace;letter-spacing:.06em;text-decoration:none;
  color:var(--accent2);padding:7px 9px;border:1px solid var(--line);border-radius:8px;background:var(--panel2);transition:.18s}
.mini-links a:hover{transform:translateY(-2px);border-color:var(--accent);box-shadow:0 6px 16px var(--glow)}
.vodwrap{position:relative;border-radius:10px;overflow:hidden}
#vod{width:100%;border-radius:10px;background:#04070d;display:block;max-height:190px}
.vod-empty{position:absolute;inset:0;display:grid;place-content:center;text-align:center;pointer-events:none;
  font:700 10px/1 ui-monospace,monospace;letter-spacing:.14em;color:var(--dim);
  background:linear-gradient(145deg,#080d18,#101a2c);transition:opacity .45s}
.vod-empty span{display:block;width:32px;height:32px;margin:0 auto 12px;border-radius:50%;
  border:1px dashed var(--line2);animation:spin 9s linear infinite}
.vod-empty.hide{opacity:0}
/* ---------- log ---------- */
.logcard{margin-top:18px;padding:0;overflow:hidden;animation:rise .6s .3s both}
.loghd{display:flex;align-items:center;gap:9px;padding:13px 18px;border-bottom:1px solid var(--line)}
.loghd h2{margin:0;font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:var(--muted);font-weight:700}
.loghd .sp{flex:1}
.leds{display:flex;gap:6px;margin-right:6px}
.leds i{width:9px;height:9px;border-radius:50%;display:block}
.leds i:nth-child(1){background:#ff5f57}.leds i:nth-child(2){background:#febc2e}.leds i:nth-child(3){background:#28c840}
#log{height:232px;overflow:auto;padding:12px 18px 16px;
  font:12px/1.65 ui-monospace,SFMono-Regular,Consolas,monospace;background:var(--panel2)}
#log::-webkit-scrollbar{width:8px}#log::-webkit-scrollbar-thumb{background:var(--line2);border-radius:9px}
#log::after{content:'_';display:inline-block;color:var(--accent2);font-weight:700;animation:caret 1s steps(1) infinite}
@keyframes caret{50%{opacity:0}}
.line{display:flex;gap:10px;padding:2px 8px;border-left:2px solid transparent;animation:slidein .3s both}
.line .ts{color:var(--dim);flex:none}.line .tg{flex:none;width:44px;color:var(--accent2);font-weight:700}
.line .tx{word-break:break-word;color:var(--muted)}
.line.OK .tg,.line.OK .tx{color:var(--good)}
.line.ERR{background:rgba(255,96,112,.07);border-left-color:var(--bad)}
.line.ERR .tg,.line.ERR .tx{color:var(--bad)}
.line.WARN .tg,.line.WARN .tx{color:var(--warn)}
@keyframes slidein{from{opacity:0;transform:translateX(-10px)}to{opacity:1;transform:none}}
@keyframes rise{from{opacity:0;transform:translateY(16px)}to{opacity:1;transform:none}}
.foot{text-align:center;margin-top:20px;font:600 10px/1 ui-monospace,monospace;
  letter-spacing:.14em;color:var(--dim);text-transform:uppercase}
.toast{position:fixed;right:24px;bottom:24px;z-index:99;padding:12px 18px;border-radius:12px;
  border:1px solid var(--line2);background:var(--panel2);color:var(--ink);font-weight:600;font-size:12px;
  box-shadow:var(--shadow);opacity:0;transform:translateY(14px) scale(.96);pointer-events:none;
  transition:opacity .28s,transform .28s}
.toast.show{opacity:1;transform:none}
.toast.good{border-color:rgba(60,230,162,.5)}.toast.bad{border-color:rgba(255,96,112,.5)}
html[data-theme="light"] .stat .k,html[data-theme="light"] .foot{color:#5a6b8c}
html[data-theme="light"] #log .line .tx{color:#33415e}
html[data-theme="light"] #log .line .ts{color:#6a7a98}
html[data-theme="light"] #log .line .tg{color:#08788f}
html[data-theme="light"] .line.OK .tg,html[data-theme="light"] .line.OK .tx{color:#0a7d54}
html[data-theme="light"] .vod-empty{color:#a3b4d2}
@media(prefers-reduced-motion:reduce){*{animation-duration:.001s!important;transition-duration:.001s!important}}
</style>
</head>
<body>
<div class="aurora"><i></i><i></i><i></i></div>
<div class="grid"></div>

<div class="wrap">
  <header class="top panel">
    <div class="brand">
      <div class="logo"></div>
      <div>
        <h1>RTSP Monitor <span style="color:var(--accent2)">/ Live Console</span></h1>
        <p>hls-debug-player-v5 &middot; zero external requests</p>
      </div>
    </div>
    <span class="live-badge"><i class="dot"></i> LIVE</span>
    <span class="clock" id="clock">--:--:--</span>
    <div class="tools">
      <button class="btn" id="themeBtn" title="Toggle theme">THEME</button>
      <button class="btn" id="snapBtn" title="Save current frame as PNG">SNAPSHOT</button>
      <button class="btn" id="fsBtn" title="Fullscreen video">FULLSCREEN</button>
    </div>
  </header>

  <div class="main">
    <section class="panel card">
      <div class="hd"><h2>Live stream</h2><span class="sp"></span><span class="chip" id="stateChip">INIT</span></div>
      <div class="screen" id="screen">
        <video id="live" controls autoplay muted playsinline></video>
        <span class="corner tl"></span><span class="corner tr"></span>
        <span class="corner bl"></span><span class="corner br"></span>
        <div class="scan"></div>
        <div class="hud"><span class="tag"><i></i> LIVE</span><span class="rec" id="hudClock">00:00:00</span><span class="r" id="hudRes">---- x ----</span></div>
        <div class="spinner" id="spinner"><b></b><span>WAITING FOR STREAM</span></div>
      </div>
      <div class="under">
        <span>PLAYLIST</span><a href="hls/stream.m3u8" target="_blank" rel="noopener">hls/stream.m3u8</a>
        <span class="sp" style="flex:1"></span><span>BUFFER</span>
        <span class="bar"><i id="barFill"></i></span>
      </div>
    </section>

    <aside>
      <div class="panel card">
        <div class="hd"><h2>Telemetry</h2><span class="sp"></span><span class="chip" id="rateChip">-- FPS</span></div>
        <div class="stats">
          <div class="stat"><div class="k">Resolution</div><div class="v" id="sRes">--</div></div>
          <div class="stat"><div class="k">Buffer</div><div class="v" id="sBuf">0.0<small>s</small></div></div>
          <div class="stat"><div class="k">Latency</div><div class="v" id="sLat">--<small>s</small></div></div>
          <div class="stat"><div class="k">Segments</div><div class="v" id="sSeg">0</div></div>
        </div>
        <div class="heart off" id="heart">
          <i></i><i></i><i></i><i></i><i></i><i></i><i></i><i></i><i></i><i></i><i></i><i></i>
        </div>
      </div>

      <div class="panel card">
        <div class="hd"><h2>Latest motion event</h2></div>
        <div class="vodwrap">
          <video id="vod" controls playsinline></video>
          <div class="vod-empty" id="vodEmpty"><span></span>NO EVENT RECORDED YET</div>
        </div>
        <div class="mini-links">
          <a href="stream.mp4">stream.mp4</a><a href="latest.json">latest.json</a>
          <a href="latest.jpg">latest.jpg</a><a href="hls.min.js">hls.min.js</a>
        </div>
      </div>
    </aside>
  </div>

  <section class="panel logcard">
    <div class="loghd">
      <div class="leds"><i></i><i></i><i></i></div>
      <h2>Diagnostics</h2><span class="sp"></span>
      <button class="btn" id="copyBtn">COPY</button>
      <button class="btn" id="clearBtn">CLEAR</button>
    </div>
    <div id="log"></div>
  </section>

  <div class="foot">RTSP Monitor &middot; HLS over CDN origin &middot; local-only assets</div>
</div>
<div class="toast" id="toast"></div>

<script>
(function(){
'use strict';
function $(id){return document.getElementById(id);}
var out=$('log'), raw=[];
function pad(n){return n<10?'0'+n:''+n;}
function stamp(){var d=new Date();return pad(d.getHours())+':'+pad(d.getMinutes())+':'+pad(d.getSeconds());}
function esc(s){return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');}
function say(s,tag){
  var t=stamp(), tg=tag||'INFO';
  raw.push(t+' ['+tg+'] '+s);
  var d=document.createElement('div');
  d.className='line '+(tg==='ERR'?'ERR':tg==='OK'?'OK':tg==='WARN'?'WARN':'');
  d.innerHTML='<span class="ts">'+t+'</span><span class="tg">'+tg+'</span><span class="tx">'+esc(s)+'</span>';
  out.appendChild(d);
  while(out.childElementCount>200){out.removeChild(out.firstChild);}
  out.scrollTop=out.scrollHeight;
}
function ok(s){say(s,'OK');}
function fail(s){say(s,'ERR');}
var toastTimer;
function toast(msg,kind){
  var t=$('toast');t.textContent=msg;t.className='toast show '+(kind||'');
  clearTimeout(toastTimer);toastTimer=setTimeout(function(){t.className='toast';},2300);
}
function setState(txt,kind){
  var c=$('stateChip');c.textContent=txt;c.className='chip '+(kind||'');
  var h=$('heart');
  h.className='heart '+(kind==='good'?'':kind==='bad'?'bad':'off');
}

/* ---- CSP self-diagnosis -------------------------------------------
   hls.js attaches MediaSource by assigning video.src = "blob:...".
   A CSP with only "default-src 'self'" (no media-src) blocks that, so
   sourceopen never fires and NOT A SINGLE .ts request is ever made.
   Listen for the browser's own violation event so the page explains the
   real cause instead of silently stalling at "attaching media". */
var cspBlocked=false;
document.addEventListener('securitypolicyviolation',function(e){
  var uri=String(e.blockedURI||''), dir=String(e.violatedDirective||'');
  fail('CSP BLOCKED '+dir+' -> '+uri);
  if(/blob:/i.test(uri)||/media-src|default-src/i.test(dir)){
    fail("FIX: add media-src 'self' blob: + worker-src 'self' blob: + connect-src 'self' to your Content-Security-Policy header. Without blob: media, hls.js can NEVER request a .ts");
    setState('CSP BLOCKED','bad');
    toast('CSP blocked blob: media','bad');
    cspBlocked=true;
  }
});

/* ---- clock + telemetry ---- */
var hlsRef=null, segCount=0, lastFrames=0, lastAt=0;
function tick(){
  var d=new Date(), t=pad(d.getHours())+':'+pad(d.getMinutes())+':'+pad(d.getSeconds());
  $('clock').textContent=t;$('hudClock').textContent=t;
  var v=$('live');
  if(v.videoWidth){
    $('sRes').textContent=v.videoWidth+' x '+v.videoHeight;
    $('hudRes').textContent=v.videoWidth+' x '+v.videoHeight+'  |  '+t;
  }
  try{
    var q=v.getVideoPlaybackQuality?v.getVideoPlaybackQuality():null;
    if(q&&lastAt){
      var f=q.totalVideoFrames-lastFrames, dt=(performance.now()-lastAt)/1000;
      if(dt>0.4){$('rateChip').textContent=Math.max(0,Math.round(f/dt))+' FPS';}
    }
    if(q){lastFrames=q.totalVideoFrames;lastAt=performance.now();}
  }catch(e){}
  try{
    if(v.buffered&&v.buffered.length){
      var b=Math.max(0,v.buffered.end(v.buffered.length-1)-v.currentTime);
      $('sBuf').innerHTML=b.toFixed(1)+'<small>s</small>';
      $('barFill').style.width=Math.min(100,b/10*100).toFixed(1)+'%';
    }
  }catch(e){}
  try{
    if(hlsRef&&typeof hlsRef.latency==='number'&&isFinite(hlsRef.latency)){
      $('sLat').innerHTML=hlsRef.latency.toFixed(1)+'<small>s</small>';
    }
  }catch(e){}
  $('sSeg').textContent=String(segCount);
}
setInterval(tick,1000);tick();

/* ---- toolbar ---- */
function setTheme(t){
  document.documentElement.setAttribute('data-theme',t);
  try{localStorage.setItem('rtsp_theme',t);}catch(e){}
  $('themeBtn').textContent=(t==='dark'?'LIGHT':'DARK')+' MODE';
}
try{setTheme(localStorage.getItem('rtsp_theme')||'dark');}catch(e){setTheme('dark');}
$('themeBtn').onclick=function(){
  var n=document.documentElement.getAttribute('data-theme')==='dark'?'light':'dark';
  setTheme(n);toast('Theme: '+n,'good');say('theme switched to '+n);
};
$('fsBtn').onclick=function(){
  var s=$('screen');
  if(document.fullscreenElement){document.exitFullscreen();return;}
  if(s.requestFullscreen){var p=s.requestFullscreen();if(p&&p.catch){p.catch(function(){});}}
  else{fail('fullscreen not supported by this browser');}
};
$('snapBtn').onclick=function(){
  var v=$('live');
  if(!v.videoWidth){toast('No video frame yet','bad');fail('snapshot: no decoded frame');return;}
  try{
    var c=document.createElement('canvas');c.width=v.videoWidth;c.height=v.videoHeight;
    c.getContext('2d').drawImage(v,0,0,c.width,c.height);
    var a=document.createElement('a');a.download='snapshot_'+Date.now()+'.png';
    a.href=c.toDataURL('image/png');a.click();
    ok('snapshot saved '+c.width+'x'+c.height);toast('Snapshot saved','good');
  }catch(e){fail('snapshot failed: '+e);toast('Snapshot blocked','bad');}
};
$('clearBtn').onclick=function(){out.innerHTML='';raw=[];say('log cleared');};
$('copyBtn').onclick=function(){
  var txt=raw.join('\n');
  try{
    if(navigator.clipboard&&navigator.clipboard.writeText){
      navigator.clipboard.writeText(txt).then(function(){toast('Log copied','good');},function(){fail('clipboard blocked');});
    }else{fail('clipboard API unavailable (needs HTTPS or localhost)');}
  }catch(e){fail('clipboard failed: '+e);}
};

/* ---- LOCAL ONLY. This page must NEVER contact the public internet. ---- */
var video=$('live');
var LOCAL_JS='./hls.min.js';
var candidates=['hls/stream.m3u8','stream.m3u8'];
var nativeSupport='';
var lastFragAt=Date.now();
var rebuildTimer=null, rebuildCount=0;
var isVodPlaylist=false;
var MAX_LOCAL_TRIES=6;
var NO_FRAG_REBUILD_MS=12000;

function flash(){var s=$('screen');s.classList.remove('flash');void s.offsetWidth;s.classList.add('flash');}
function hideSpinner(){$('spinner').classList.add('hide');}

/* Retry the SAME local URL; there is deliberately no CDN fallback.
   EdgeOne truncates a single response at ~150KB / ~13s (verified: bytes
   0-65535 and 65536-131071 return 206, but a 476KB range resets at ~148KB).
   So try a whole-file <script> first; if it stalls or the returned JS is
   truncated (window.Hls missing), fall back to fetching the file in 64KB
   Range chunks which each complete, then run it inline. */
var booted=false;
var CHUNK_SIZE=64*1024;
function bootPlayer(){
  if(booted){return;}
  booted=true;
  boot();
}
function loadLocalHls(attempt){
  if(window.Hls){ok('hls.js ready');bootPlayer();return;}
  if(attempt>=MAX_LOCAL_TRIES){
    fail('hls.min.js unreachable after '+MAX_LOCAL_TRIES+' LOCAL attempts; NO external CDN will be tried');
    fail('fix: copy hls.min.js next to rtsp_monitor.py on the SCRIPT host, restart it, then hard-refresh');
    setState('NO PLAYER','bad');toast('hls.min.js missing','bad');
    return;
  }
  var wait=attempt===0?0:Math.min(5000,600*Math.pow(2,attempt-1));
  setTimeout(function(){
    say('loading hls.js (attempt '+(attempt+1)+'/'+MAX_LOCAL_TRIES+'): '+LOCAL_JS);
    var wholeFailed=false, timer=null;
    function giveUpWhole(reason){
      if(wholeFailed){return;}
      wholeFailed=true;
      if(timer){clearTimeout(timer);timer=null;}
      say('whole-file load '+reason+' -> chunked range fetch','WARN');
      loadChunked(0);
    }
    var s=document.createElement('script');
    s.src=LOCAL_JS+(attempt?'?retry='+attempt+'&t='+Date.now():'');
    timer=setTimeout(function(){giveUpWhole('timed out (10s, likely truncated by CDN)');},10000);
    s.onload=function(){
      if(wholeFailed){return;}
      if(timer){clearTimeout(timer);timer=null;}
      if(window.Hls){ok('hls.js loaded (whole file)');bootPlayer();}
      else{giveUpWhole('loaded but window.Hls undefined (truncated/corrupt)');}
    };
    s.onerror=function(){giveUpWhole('request failed (no status, likely (failed) 0 bytes)');};
    document.head.appendChild(s);
  },wait);
}
/* Fetch hls.min.js in 64KB Range chunks (each completes on EdgeOne),
   concatenate, then execute inline. CSP allows inline scripts
   (script-src 'self' 'unsafe-inline'), so textContent execution works. */
function loadChunked(badTries){
  badTries=badTries||0;
  say('chunked load: fetching hls.min.js in '+(CHUNK_SIZE/1024)+'KB ranges');
  fetch(LOCAL_JS,{method:'GET',headers:{'Range':'bytes=0-0'},cache:'no-store'})
    .then(function(r){
      if(r.status!==206){throw new Error('no range support (status '+r.status+')');}
      var cr=r.headers.get('content-range')||'';
      var m=cr.match(/\/(\d+)\s*$/);
      var total=m?parseInt(m[1],10):0;
      if(!total){throw new Error('no total size in content-range: '+cr);}
      say('chunked load: total='+total+' bytes in '+Math.ceil(total/CHUNK_SIZE)+' parts');
      var bufs=[], got=0;
      function next(start){
        if(start>=total){assemble();return;}
        var end=Math.min(start+CHUNK_SIZE-1,total-1);
        fetch(LOCAL_JS,{headers:{'Range':'bytes='+start+'-'+end},cache:'no-store'})
          .then(function(rr){return rr.arrayBuffer();})
          .then(function(ab){
            var u=new Uint8Array(ab);
            bufs.push(u);got+=u.length;
            if(u.length<CHUNK_SIZE||got>=total){assemble();}
            else{next(start+CHUNK_SIZE);}
          })
          .catch(function(e){chunkFail('range fetch error: '+e);});
      }
      function assemble(){
        try{
          var all=new Uint8Array(total),p=0,i;
          for(i=0;i<bufs.length;i++){all.set(bufs[i],p);p+=bufs[i].length;}
          var code=new TextDecoder('utf-8').decode(all);
          var sc=document.createElement('script');
          sc.textContent=code;
          document.head.appendChild(sc);
          if(window.Hls){ok('hls.js loaded (chunked, '+bufs.length+' parts)');bootPlayer();}
          else{chunkFail('assembled but window.Hls undefined (still truncated)');}
        }catch(e){chunkFail('assemble error: '+e);}
      }
      next(0);
    })
    .catch(function(e){chunkFail('range head error: '+e);});
  function chunkFail(msg){
    say('chunked load FAILED: '+msg,'WARN');
    if(badTries<MAX_LOCAL_TRIES){
      say('retrying chunked load (round '+(badTries+1)+')','WARN');
      setTimeout(function(){loadChunked(badTries+1);},1500);
    }else{
      fail('hls.min.js could not be loaded even chunked; check origin reachability');
      setState('NO PLAYER','bad');
    }
  }
}

/* One pass over the candidate paths. cb(null) means "not ready YET". */
function probeOnce(i,cb){
  if(i>=candidates.length){cb(null);return;}
  var url=candidates[i];
  fetch(url,{cache:'no-store'}).then(function(r){
    if(!r.ok){say('GET '+url+' -> HTTP '+r.status);return probeOnce(i+1,cb);}
    return r.text().then(function(txt){
      var segs=txt.split('\n').filter(function(l){return /\.ts\s*$/.test(l.trim());});
      say('GET '+url+' -> HTTP '+r.status+'  bytes='+txt.length+'  segments='+segs.length);
      if(!segs.length){return probeOnce(i+1,cb);}
      cb({url:url,segs:segs});
    });
  }).catch(function(e){
    say('GET '+url+' failed: '+e,'WARN');return probeOnce(i+1,cb);
  });
}

/* A transient 404 while FFmpeg is warming up must NOT kill playback. */
function findPlaylist(cb,attempt){
  attempt=attempt||0;
  setState(attempt?'WAITING '+attempt:'DETECTING','warn');
  probeOnce(0,function(res){
    if(res){
      ok('playlist ready: '+res.url+'  segments='+res.segs.length);
      var base=res.url.slice(0,res.url.lastIndexOf('/')+1);
      var segUrl=base+res.segs[res.segs.length-1].trim();
      fetch(segUrl,{method:'HEAD',cache:'no-store'}).then(function(sr){
        if(sr.ok){ok('segment reachable: '+segUrl.split('/').pop()+' -> HTTP '+sr.status);}
        else{fail('segment UNREACHABLE: '+segUrl.split('/').pop()+' -> HTTP '+sr.status);}
      }).catch(function(e){fail('segment HEAD failed: '+e);});
      cb(res.url);return;
    }
    if(attempt===0||attempt%5===0){
      say('no usable playlist yet (attempt '+(attempt+1)+'); FFmpeg may still be cutting the first segment','WARN');
    }
    setTimeout(function(){findPlaylist(cb,attempt+1);},Math.min(3000,500+attempt*250));
  });
}

function scheduleRebuild(ms){
  if(rebuildTimer||cspBlocked){return;}
  rebuildCount++;
  setState('RECONNECTING','warn');
  rebuildTimer=setTimeout(function(){
    rebuildTimer=null;
    say('rebuilding player (round '+rebuildCount+')','WARN');
    try{if(hlsRef){hlsRef.destroy();}}catch(e){}
    hlsRef=null;
    isVodPlaylist=false;
    lastFragAt=Date.now();
    findPlaylist(function(u){startPlayer(u);},0);
  },ms||1200);
}

function startPlayer(url){
  var canMSE=!!(window.Hls&&Hls.isSupported());
  if(!canMSE&&nativeSupport){
    ok('hls.js unavailable -> falling back to native HLS');setState('NATIVE HLS','good');
    video.src=url;hideSpinner();return;
  }
  if(!window.Hls){fail('hls.js MISSING -> no src, browser will never request a .ts');setState('NO PLAYER','bad');return;}
  if(!Hls.isSupported()){fail('Hls.isSupported() is false (MSE unavailable)');setState('MSE UNAVAILABLE','bad');return;}

  try{if(hlsRef){hlsRef.destroy();}}catch(e){}
  /* liveSyncDurationCount=3 keeps 3 segments of safety margin: one
     missing .ts (edge MISS after delete_segments removed it) must not
     immediately stall playback. With 2s segments this is ~6s latency,
     the same as the old 2 x 3s segments. */
  var h=new Hls({
    lowLatencyMode:true,
    liveSyncDurationCount:3,
    maxLiveSyncPlaybackRate:1.2,
    debug:false,
    manifestLoadingMaxRetry:6,
    manifestLoadingRetryDelay:600,
    levelLoadingMaxRetry:6,
    levelLoadingRetryDelay:600,
    fragLoadingMaxRetry:5,
    fragLoadingRetryDelay:400,
    startFragPrefetch:true,
    backBufferLength:30
  });
  hlsRef=h;
  lastFragAt=Date.now();
  h.on(Hls.Events.MEDIA_ATTACHED,function(){say('media attached -> loadSource '+url);h.loadSource(url);});
  h.on(Hls.Events.MANIFEST_PARSED,function(e,d){
    ok('MANIFEST_PARSED levels='+d.levels.length);setState('CONNECTING','warn');
    var p=video.play();
    if(p&&p.catch){p.catch(function(err){say('autoplay blocked ('+err.name+'); click play','WARN');setState('CLICK PLAY','warn');});}
  });
  h.on(Hls.Events.LEVEL_LOADED,function(e,d){
    /* hls.js 1.6.x exposes LevelDetails.fragments, NOT .segments.
       Reading .segments.length here used to throw; hls.js catches listener
       exceptions and re-emits them as "internalException fatal=false" on
       every live playlist refresh, which also skipped the VOD detection. */
    var det=d.details||{};
    var frags=det.fragments||det.segments||[];
    say('LEVEL_LOADED segments='+frags.length);
    segCount=frags.length;
    isVodPlaylist=(det.live===false);
    if(isVodPlaylist){say('playlist has ENDLIST -> stall watchdog disabled','WARN');}
  });
  h.on(Hls.Events.FRAG_LOADED,function(e,d){
    lastFragAt=Date.now();
    ok('SEGMENT LOADED '+String(d.frag.url).split('/').pop());
    flash();hideSpinner();setState('LIVE','good');
  });
  h.on(Hls.Events.FRAG_BUFFERED,function(){hideSpinner();});
  h.on(Hls.Events.ERROR,function(e,d){
    fail('HLS ERROR type='+d.type+' details='+d.details+' fatal='+d.fatal+(d.response?' http='+d.response.code:''));
    if(!d.fatal){return;}
    var typ=String(d.type||''), det=String(d.details||'');
    if(typ==='networkError'){
      if(/manifest|levelLoad/i.test(det)){
        say('manifest/level fatal -> rebuild','WARN');scheduleRebuild(1200);
      }else{
        say('fragment network error -> startLoad() again','WARN');
        try{h.startLoad();}catch(err){scheduleRebuild(1200);}
      }
    }else if(typ==='mediaError'){
      say('media error -> recoverMediaError()','WARN');
      try{h.recoverMediaError();}catch(err){scheduleRebuild(1200);}
    }else{
      say('other fatal -> rebuild','WARN');scheduleRebuild(1200);
    }
  });
  say('Hls created, attaching media...');setState('CONNECTING','warn');
  h.attachMedia(video);
}

function boot(){
  nativeSupport=video.canPlayType('application/vnd.apple.mpegurl');
  say('native HLS support: '+(nativeSupport||'no')+' (not trusted on Chromium; MSE takes priority)');
  say('window.Hls: '+(window.Hls?'yes':'no')+'  Hls.isSupported(): '+(window.Hls?Hls.isSupported():'n/a'));
  findPlaylist(function(u){startPlayer(u);},0);
  refreshVod();
}

function refreshVod(){
  fetch('latest.json',{cache:'no-store'}).then(function(r){return r.json();}).then(function(j){
    if(j&&j.url){$('vod').src=j.url;$('vodEmpty').classList.add('hide');}
  }).catch(function(){});
}

/* If no .ts arrived for 12s the player is stuck (the "no ts requests"
   symptom). Rebuild it instead of waiting forever. */
setInterval(function(){
  if(!hlsRef||rebuildTimer||isVodPlaylist){return;}
  if(Date.now()-lastFragAt>NO_FRAG_REBUILD_MS){
    fail('NO SEGMENT for '+(NO_FRAG_REBUILD_MS/1000)+'s -> forcing player rebuild');
    scheduleRebuild(500);
  }
},4000);
setInterval(refreshVod,15000);

say('console ready - hls-debug-player-v7 - ZERO external requests');
loadLocalHls(0);
})();
</script>
</body>
</html>
"""


class MediaPublisher:
    """
    Publishes finished media into a plain directory.

    CDN-correct layout:
      * immutable, uniquely named files in events/ -> cache forever
      * one small no-cache pointer (latest.json) -> always fresh
      * a fixed stream.mp4 alias -> the URL the user asked for, but it
        MUST be served no-cache (or no-store) or a CDN will keep serving
        the previous event under the same name.
    """

    def __init__(self, root_text: str):
        self.root = ensure_dir(root_text)
        # Build subdirs RELATIVE to the resolved root. Using ensure_dir()
        # for them could silently fall back to /tmp/<name> and split the
        # HLS files away from the events / index.html if the parent is
        # not writable.
        self.hls_dir = self.root / "hls"
        self.events_dir = self.root / "events"
        try:
            self.hls_dir.mkdir(parents=True, exist_ok=True)
            self.events_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            log(f"publish: cannot create subdirectories: {exc}")
        if PUBLISH_ENABLED:
            try:
                player = self.root / "index.html"
                # Upgrade an older player page in place. The old one loaded
                # hls.js ONLY from a public CDN, which silently leaves the
                # <video> with no src on an offline LAN: the browser then
                # never requests a single .ts segment.
                needs_write = True
                if player.exists():
                    try:
                        current = player.read_text(
                            encoding="utf-8",
                            errors="ignore",
                        )
                        needs_write = PLAYER_MARKER not in current
                    except OSError:
                        needs_write = False
                if needs_write:
                    _atomic_write_bytes(
                        player,
                        PLAYER_PAGE.encode("ascii"),
                    )
            except OSError as exc:
                log(f"publish: player page write failed: {exc}")

            # Vendor hls.js into the served directory so a player on an
            # OFFLINE LAN does not depend on a public CDN. Put hls.min.js
            # next to this script; it is published on every startup.
            try:
                src = SCRIPT_DIR / "hls.min.js"
                if not src.is_file():
                    log(
                        "publish: WARNING - SCRIPT_DIR/hls.min.js is MISSING. "
                        "The v4 player has NO external CDN fallback, so the "
                        "web page will never request a .ts segment. Copy "
                        "hls.min.js next to this script and restart."
                    )
                if src.is_file():
                    dst = self.root / "hls.min.js"
                    if (
                        not dst.is_file()
                        or dst.stat().st_size != src.stat().st_size
                    ):
                        _atomic_copy_file(str(src), dst)
                        log("publish: bundled hls.min.js (offline player)")

                    # Pre-compressed sibling for "gzip_static on".
                    #
                    # The origin uplink is the real bottleneck: this file
                    # is 541870 B raw and the measured CDN pull-back rate
                    # is only 3.6-34 KB/s, so a raw fetch hits the edge's
                    # ~13s origin timeout and the body is cut off halfway
                    # (DevTools "(failed)" with no HTTP status). gzip -9
                    # gives 162547 B, which normally fits the budget.
                    try:
                        gz_bytes = _gzip_bytes(src.read_bytes())
                        gz_dst = self.root / "hls.min.js.gz"
                        if (
                            not gz_dst.is_file()
                            or gz_dst.stat().st_size != len(gz_bytes)
                        ):
                            _atomic_write_bytes(gz_dst, gz_bytes)
                            log(
                                f"publish: hls.min.js.gz "
                                f"{src.stat().st_size} -> {len(gz_bytes)} "
                                f"bytes (nginx gzip_static)"
                            )
                    except OSError as exc:
                        log(f"publish: hls.min.js.gz failed: {exc}")
            except OSError as exc:
                log(f"publish: hls.min.js copy failed: {exc}")

    def public_url(self, relative: str) -> str:
        if PUBLIC_BASE_URL:
            return f"{PUBLIC_BASE_URL}/{relative}"
        return relative

    def publish_event_mp4(
        self,
        mp4_path: str,
        session: "EventSession",
        motion_count: int,
    ) -> Optional[str]:
        """
        Returns the PUBLIC URL of the unique immutable event file, or None.
        Must be called BEFORE the Telegram worker takes ownership of the
        temp MP4 (it deletes the temp file after uploading).
        """
        if not PUBLISH_ENABLED:
            return None

        start = datetime.fromtimestamp(session.start_time)
        name = (
            f"event_{start.strftime('%Y%m%d_%H%M%S')}"
            f"_{session.event_id:04d}.mp4"
        )
        unique = self.events_dir / name

        _atomic_copy_file(mp4_path, unique)
        # Fixed alias: the "stream.mp4" URL. Served no-cache by nginx.
        _atomic_copy_file(str(unique), self.root / "stream.mp4")

        duration = max(
            0.0,
            (session.end_time or time.time()) - session.start_time,
        )
        size = unique.stat().st_size
        payload = {
            "event_id": session.event_id,
            "start": start.strftime("%Y-%m-%d %H:%M:%S"),
            "start_epoch": round(session.start_time, 3),
            "duration": round(duration, 2),
            "motion_regions": motion_count,
            "bytes": size,
            "url": f"events/{name}",
            "fixed_url": "stream.mp4",
            "updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        if PUBLIC_BASE_URL:
            payload["public_url"] = self.public_url(f"events/{name}")
            payload["public_fixed_url"] = self.public_url("stream.mp4")

        _atomic_write_bytes(
            self.root / "latest.json",
            (
                json.dumps(payload, ensure_ascii=True, indent=2) + "\n"
            ).encode("ascii"),
        )

        self._prune_events()
        log(
            f"published event mp4: {payload['url']} "
            f"({size / (1024 * 1024):.2f} MB) + stream.mp4"
        )
        # For an external caption we want the immutable URL, not the alias.
        return payload.get("public_url")

    def _prune_events(self) -> None:
        if PUBLISH_KEEP_EVENTS <= 0:
            return
        try:
            files = sorted(
                self.events_dir.glob("event_*.mp4"),
                key=lambda p: p.name,
            )
        except OSError:
            return
        for old in files[:-PUBLISH_KEEP_EVENTS]:
            try:
                old.unlink()
            except OSError:
                pass

    def publish_snapshot(self, jpeg_bytes: bytes) -> None:
        if not PUBLISH_ENABLED or not PUBLISH_LATEST_JPG:
            return
        try:
            _atomic_write_bytes(
                self.root / "latest.jpg",
                jpeg_bytes,
            )
        except OSError as exc:
            log(f"publish: latest.jpg failed: {exc}")


def cleanup_hls_dir(hls_dir: Path) -> None:
    """
    Remove genuinely stale segments, but keep recent ones so a viewer that
    cached the old playlist does not get an instant 404 on a restart.
    Epoch-based numbering already guarantees no URL is ever reused.
    """
    max_age = max(60.0, HLS_TIME * max(1, HLS_LIST_SIZE) * 4)
    now = time.time()
    try:
        items = list(hls_dir.iterdir())
    except OSError:
        return
    for item in items:
        try:
            if not item.is_file():
                continue
            if item.name.endswith(".tmp"):
                item.unlink()
                continue
            if item.name.startswith("stream_") and item.suffix == ".ts":
                if now - item.stat().st_mtime > max_age:
                    item.unlink()
        except OSError:
            pass


def build_hls_command(hls_dir: Path) -> list:
    """
    rawvideo pipe -> H.264 -> HLS.

    The input is the ALREADY DECODED, ALREADY ANNOTATED BGR frame that the
    main loop pushes into the UDP encoder. There is no RTSP input here, so
    the camera only ever serves ONE session. Keep the option list aligned
    with the offline HLS probe: untested muxer flags must not be added.
    """
    playlist = hls_dir / "stream.m3u8"
    segment = hls_dir / "stream_%06d.ts"
    gop = max(1, int(round(HLS_FPS * HLS_TIME)))

    command = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "warning",
        "-y",

        # Dedicated writer thread pushes annotated BGR frames in real time.
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-framerate", str(HLS_FPS),
        # Anchor PTS to the REAL arrival time of each frame. Without this
        # the rawvideo demuxer counts FRAMES, so dropped frames compress
        # the timeline (measured: 6.5s of media over 11s of wall clock).
        "-use_wallclock_as_timestamps", "1",
        "-i", "pipe:0",

        "-map", "0:v",
        "-an",
        "-c:v", HLS_H264_ENCODER,
        "-pix_fmt", "yuv420p",
        "-b:v", HLS_VIDEO_BITRATE,
        # Force an IDR every HLS_TIME seconds so segments really are
        # HLS_TIME long and each starts with a keyframe.
        "-g", str(gop),
        "-keyint_min", str(gop),
        "-sc_threshold", "0",
    ]
    if HLS_H264_ENCODER == "libx264":
        command += [
            "-preset", HLS_X264_PRESET,
            "-tune", "zerolatency",
        ]

    command += [
        "-f", "hls",
        "-hls_time", f"{HLS_TIME:g}",
        "-hls_list_size", str(HLS_LIST_SIZE),
        # temp_file         -> atomic playlist/segment rename (CDN safe)
        # omit_endlist      -> stays a LIVE playlist, no ENDLIST
        # independent_segments -> every .ts can be cached/decoded alone
        # delete_segments   -> origin dir does not grow forever
        "-hls_flags",
        "delete_segments+omit_endlist+independent_segments+temp_file",
        # epoch_us numbering: a restart NEVER reuses a segment URL with
        # different bytes, which would poison a CDN edge cache. Plain
        # "epoch" (seconds) is not enough - a restart within the same
        # second range can still collide. Microseconds cannot.
        "-hls_start_number_source", "epoch_us",
        "-hls_segment_filename", str(segment),
        "-hls_allow_cache", "1",
        str(playlist),
    ]
    return command


class HlsPublisher:
    """
    Queue-isolated H.264 -> HLS pipeline.

    submit() is called from the MAIN LOOP and must never block: it only
    replaces the single queued frame. ONE dedicated thread owns the FFmpeg
    process and its stdin, so a slow disk / stalled origin can never
    backpressure the low-latency UDP/ESP32 path.
    """

    def __init__(self, hls_dir: Path):
        self.hls_dir = hls_dir
        self.playlist = hls_dir / "stream.m3u8"
        self.queue = queue.Queue(maxsize=HLS_QUEUE_SIZE)
        self.process = None
        self.thread = None
        self._dropped = 0

    def submit(self, payload: bytes) -> None:
        """Non-blocking latest-frame handoff. Safe to call every frame."""
        try:
            self.queue.put_nowait(payload)
        except queue.Full:
            try:
                self.queue.get_nowait()
                self._dropped += 1
            except queue.Empty:
                pass
            try:
                self.queue.put_nowait(payload)
            except queue.Full:
                pass

    def take_dropped(self) -> int:
        n = self._dropped
        self._dropped = 0
        return n

    def start(self) -> None:
        if not (HLS_ENABLED and PUBLISH_ENABLED):
            return
        cleanup_hls_dir(self.hls_dir)
        self.thread = threading.Thread(
            target=self._supervise,
            name="hls-writer",
            daemon=True,
        )
        self.thread.start()

    def _spawn(self) -> subprocess.Popen:
        command = build_hls_command(self.hls_dir)
        if DEBUG:
            log("hls ffmpeg: " + " ".join(command))
        proc = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            bufsize=1024 * 1024,
        )
        threading.Thread(
            target=stderr_reader,
            args=(proc, "HLS"),
            daemon=True,
        ).start()
        return proc

    def _supervise(self) -> None:
        delay = HLS_RESTART_DELAY
        while not STOP_EVENT.is_set():
            # Called only while NO ffmpeg process is running, so deleting
            # stale .tmp files here is safe. It also keeps the origin dir
            # bounded across many restarts.
            cleanup_hls_dir(self.hls_dir)
            started_at = time.monotonic()
            try:
                # Keep a LOCAL reference: close() may set self.process to
                # None from the main thread while we are still writing.
                proc = self._spawn()
                self.process = proc
            except Exception as exc:
                log(
                    f"hls ffmpeg start failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                if STOP_EVENT.wait(delay):
                    break
                delay = min(
                    delay * 1.5,
                    HLS_RESTART_MAX_DELAY,
                )
                continue

            # Pump queued frames into the encoder stdin. This is the ONLY
            # place that may block; the main loop never waits on it. Frames
            # arriving while we are blocked are replaced in the 1-slot
            # queue, so HLS always encodes "now", never a backlog.
            while not STOP_EVENT.is_set():
                if proc.poll() is not None:
                    break
                try:
                    payload = self.queue.get(timeout=0.25)
                except queue.Empty:
                    continue
                try:
                    proc.stdin.write(payload)
                    proc.stdin.flush()
                except (BrokenPipeError, OSError, ValueError):
                    break

            if STOP_EVENT.is_set():
                break

            rc = proc.poll()
            if time.monotonic() - started_at > 30.0:
                delay = HLS_RESTART_DELAY
            log(
                f"hls ffmpeg exited (rc={rc}); "
                f"restarting in {delay:.0f}s"
            )
            self._kill()
            if STOP_EVENT.wait(delay):
                break
            delay = min(delay * 1.5, HLS_RESTART_MAX_DELAY)

    def _kill(self) -> None:
        proc = self.process
        self.process = None
        if proc is None:
            return
        try:
            if proc.stdin:
                proc.stdin.close()
        except Exception:
            pass
        try:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    proc.kill()
        except Exception:
            pass

    def close(self) -> None:
        self._kill()
        # A stopped publisher must not leave a "live" playlist behind.
        try:
            self.playlist.unlink()
        except OSError:
            pass
        if self.thread is not None and self.thread.is_alive():
            try:
                self.thread.join(timeout=2.0)
            except Exception:
                pass


def start_input_ffmpeg() -> subprocess.Popen:
    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-rtsp_transport", RTSP_TRANSPORT,
        "-fflags", "+nobuffer+discardcorrupt+genpts",
        "-max_delay", "0",
        "-flags", "low_delay",
        "-use_wallclock_as_timestamps", "1",
        "-probesize", "50000",
        "-analyzeduration", "100000",
    ]

    # RTSP demuxer socket I/O timeout (microseconds). This is what makes
    # FFmpeg actually exit (instead of blocking forever) when the upstream
    # connection stalls, e.g. right after the camera/RTSP server restarts.
    try:
        timeout_us = int(RTSP_IO_TIMEOUT_US) if RTSP_IO_TIMEOUT_US else 0
    except ValueError:
        timeout_us = 0

    if timeout_us > 0:
        command += ["-timeout", str(timeout_us)]

    if INPUT_HW_DECODER:
        # Hardware video decoder. Decoder selection MUST come before -i.
        command += ["-c:v", INPUT_HW_DECODER]

    command += [
        "-i", RTSP_URL,

        # Video -> raw BGR.
        "-map", "0:v:0",
        "-vf", f"fps={INPUT_FPS},scale={WIDTH}:{HEIGHT}:flags=fast_bilinear",
        "-pix_fmt", "bgr24",
        "-f", "rawvideo",
        # The AUDIO output had this already; the rawvideo pipe did not.
        # Without it AVIO may hold back bytes of a BGR frame.
        *(["-flush_packets", "1"] if INPUT_FLUSH_PACKETS else []),
        "pipe:1",

        # Audio -> PCM -> UDP 8889.
        "-map", "0:a:0?",
        "-af", "aresample=async=1",
        "-c:a", "pcm_s16le",
        "-ar", "44100",
        "-ac", "2",
        # Prevent AVIO from holding back several PCM blocks before sending.
        "-flush_packets", "1",
        "-f", "s16le",
        UDP_AUDIO_URL,

        "-loglevel", "warning",
    ]

    if DEBUG:
        log("input ffmpeg: " + " ".join(command))

    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=1024 * 1024,
    )


def start_output_ffmpeg(rolling_dir: Path) -> subprocess.Popen:
    pattern = str(rolling_dir / "seg_%03d.mkv")

    # IMPORTANT:
    # The live UDP branch still matches the user's known-good reference
    # command (mjpeg / q=3 / yuvj420p / mpegts over UDP). Do NOT switch it
    # to H.264: the receiver expects MJPEG-in-TS. The rolling event branch
    # stays Matroska so FFmpeg can read it as a normal MJPEG stream when
    # building the MP4.
    #
    # Low-latency notes:
    #   * The input PTS is anchored to the wall clock. Without that, FFmpeg
    #     timestamps by frame COUNT and the TS timeline slowly drifts away
    #     from real time, which makes the receiver's buffer grow for as long
    #     as the stream runs (the "after a while it is 10s behind" symptom).
    #   * NO "-re". "-re" paces the input at the nominal FPS, so any backlog
    #     (one slow detection frame, a GC pause, an RTSP hiccup) gets
    #     replayed in real time and NEVER catches up. A one-off stall then
    #     becomes permanent lag. Without "-re" the encoder drains the pipe
    #     as fast as it can and latency self-heals on the next frame.
    #   * flush_packets=1 / max_delay=0 stop the mpegts and AVIO layers from
    #     holding whole JPEG frames in their buffers.
    #   * mpegts_flags=+resend_headers re-emits PAT/PMT frequently so a
    #     receiver that joins late identifies the stream quickly.
    # Keep TS overhead LOW: every extra PAT/PMT/PCR is another UDP packet
    # an MCU receiver has to interrupt on. Measured on 24 frames:
    # resend_headers alone = 131 TS packets, adding pat_pmt_at_frames +
    # pcr_period=20 = 147 (+12%). resend_headers alone is enough.
    # LIVE_MUX_TUNING=0 falls back to the plain reference mpegts branch on
    # older FFmpeg builds that do not know these child options.
    if LIVE_MUX_TUNING:
        mpegts_opts = (
            "[f=mpegts"
            ":max_delay=0"
            ":flush_packets=1"
            ":mpegts_flags=+resend_headers]"
        )
    else:
        mpegts_opts = "[f=mpegts]"

    branches = [mpegts_opts + f"'{UDP_VIDEO_URL}'"]

    # Optional mirror for an A/B control viewer on another machine.
    if UDP_VIDEO_URL_EXTRA:
        branches.append(
            mpegts_opts + f"'{UDP_VIDEO_URL_EXTRA}'"
        )

    branches.append(
        f"[f=segment:segment_time={SEGMENT_SECONDS}"
        f":segment_wrap={SEGMENT_COUNT}"
        f":reset_timestamps=1"
        f":segment_format=matroska]'{pattern}'"
    )
    tee_output = "|".join(branches)

    # See LIVE_WALLCLOCK_PTS above. This is the fix for gradual latency
    # drift: PTS gaps then equal the REAL gaps between written frames.
    wallclock_opts = (
        ["-use_wallclock_as_timestamps", "1"]
        if LIVE_WALLCLOCK_PTS else []
    )

    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-loglevel", "warning",

        # Python sends annotated BGR frames in real time, one per read.
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-framerate", str(FPS),
        *wallclock_opts,
        "-i", "pipe:0",

        "-map", "0:v",
        "-an",
        "-c:v", MJPEG_ENCODER,
        "-q:v", str(MJPEG_QUALITY),
        "-pix_fmt", "yuvj420p",

        # Prevent the muxer / AVIO layer from holding a whole frame back.
        "-max_delay", "0",
        "-flush_packets", "1",
        "-fflags", "+nobuffer+flush_packets",

        "-f", "tee",
        tee_output,
    ]

    if DEBUG:
        log("output ffmpeg: " + " ".join(command))

    return subprocess.Popen(
        command,
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        bufsize=1024 * 1024,
    )


def stderr_reader(process: subprocess.Popen, name: str) -> None:
    if process.stderr is None:
        return

    try:
        for line in iter(process.stderr.readline, b""):
            if not line:
                break
            text = line.decode("utf-8", errors="ignore").strip()
            if text:
                log(f"[{name}] {text}")
    except Exception:
        pass


def read_exact(stream, size: int) -> Optional[bytes]:
    chunks = []
    remaining = size

    while remaining:
        chunk = stream.read(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)

    return b"".join(chunks)


def read_exact_into(stream, buf: bytearray) -> bool:
    """
    Zero-copy read_exact: fills buf directly from the stream.

    Saves one full-frame bytes allocation + copy per frame (24 x 230KB/s).
    Falls back to read_exact() + slice-assign when the stream does not
    expose readinto(). Returns False on EOF (buf content is garbage then).
    """
    readinto = getattr(stream, "readinto", None)
    if readinto is None:
        data = read_exact(stream, len(buf))
        if data is None:
            return False
        buf[:len(data)] = data
        return True

    mv = memoryview(buf)
    pos = 0
    total = len(buf)
    while pos < total:
        n = readinto(mv[pos:])
        if not n:
            return False
        pos += n
    return True


class FrameClock:
    """
    Thread-safe "time of last frame received from FFmpeg" marker.

    This exists because a stalled upstream RTSP connection does not always
    make FFmpeg exit or error out (e.g. the camera reboots without sending
    a TCP FIN/RST, so the socket just goes quiet). In that case decoder.poll()
    stays None and the reader thread stays "alive" (blocked inside a
    blocking read()), so the pre-existing "process exited" reconnect check
    never fires and the monitor freezes indefinitely.

    main() checks stalled() independently of process/thread liveness and
    force-restarts the input FFmpeg when it fires.
    """

    def __init__(self) -> None:
        self._last_ts = time.monotonic()

    def touch(self) -> None:
        self._last_ts = time.monotonic()

    def stalled(self, timeout: float) -> bool:
        if timeout <= 0:
            return False
        return (time.monotonic() - self._last_ts) > timeout


class LiveStats:
    """Counters shared between the frame reader and the main loop."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._dropped = 0
        # Upstream backlog probe, filled by the reader thread.
        self._pipe_wait_sum = 0.0
        self._pipe_reads = 0
        self._pipe_instant = 0

    def add_drop(self, count: int = 1) -> None:
        with self._lock:
            self._dropped += count

    def add_pipe_read(self, wait_ms: float, instant: bool) -> None:
        with self._lock:
            self._pipe_wait_sum += wait_ms
            self._pipe_reads += 1
            if instant:
                self._pipe_instant += 1

    def take_pipe(self):
        """Return (avg read wait in ms, % of zero-wait reads)."""
        with self._lock:
            reads = self._pipe_reads
            total = self._pipe_wait_sum
            instant = self._pipe_instant
            self._pipe_wait_sum = 0.0
            self._pipe_reads = 0
            self._pipe_instant = 0
        if not reads:
            return 0.0, 0.0
        return total / reads, 100.0 * instant / reads

    def take_dropped(self) -> int:
        with self._lock:
            value = self._dropped
            self._dropped = 0
            return value


def frame_reader_worker(
    decoder,
    frame_size: int,
    frame_queue: queue.Queue,
    stop_event: threading.Event,
    reader_stop_event: threading.Event,
    frame_clock: "FrameClock",
    live_stats: "LiveStats",
    low_latency: bool,
) -> None:
    """
    Dedicated producer thread.

    LOW_LATENCY mode:
        The queue holds the newest frame only. The reader NEVER blocks on
        put(): it keeps draining FFmpeg's stdout so the OS pipe stays empty,
        and replaces any unconsumed frame with the latest one. Stale frames
        are dropped on purpose - replaying them late is exactly what turns
        a one-off slowdown into permanent latency.

    Legacy mode (LOW_LATENCY=0):
        Bounded queue with backpressure, no intentional frame dropping.

    Queued items are (writable_bgr_buffer, captured_at_monotonic), so the
    main loop can measure how old the frame it is about to send really is,
    and annotate the buffer IN PLACE without copying it first.

    If the RTSP connection reaches EOF, only this reader exits. main() then
    reconnects the input FFmpeg without stopping the output pipeline.
    """
    log("Frame reader thread started")
    queue_block_start = None

    while not stop_event.is_set() and not reader_stop_event.is_set():
        if decoder.poll() is not None:
            log("Frame reader thread: decoder exited")
            break

        # How long the read had to WAIT is the upstream backlog probe.
        # ~1000/INPUT_FPS ms => pipe empty, frames are fresh.
        # ~0 ms => pipe always has data, frames are being replayed late.
        read_start = time.monotonic()
        # Fresh writable buffer per frame: the queue may still own the
        # previous buffer while this read is already in progress.
        raw = bytearray(frame_size)
        if not read_exact_into(decoder.stdout, raw):
            if not stop_event.is_set() and not reader_stop_event.is_set():
                log("Frame reader thread: RTSP input EOF")
            break
        read_ms = (time.monotonic() - read_start) * 1000.0
        live_stats.add_pipe_read(read_ms, read_ms < PIPE_INSTANT_MS)

        captured_at = time.monotonic()

        # Data actually arrived from FFmpeg: the pipeline is alive.
        frame_clock.touch()

        item = (raw, captured_at)

        if low_latency:
            # Drop the stale frame (if any) and publish the newest one.
            # Never wait on a full queue - keep reading the pipe instead.
            try:
                frame_queue.get_nowait()
                live_stats.add_drop()
            except queue.Empty:
                pass

            try:
                frame_queue.put_nowait(item)
            except queue.Full:
                # The consumer may have taken a slot between the calls.
                # Keep whatever frame is already queued and move on.
                pass
            continue

        while not stop_event.is_set() and not reader_stop_event.is_set():
            try:
                frame_queue.put(item, timeout=FRAME_QUEUE_PUT_TIMEOUT)
                break
            except queue.Full:
                # Keep waiting. Do not discard raw.
                if queue_block_start is None:
                    queue_block_start = time.monotonic()
                    log("frame queue full; applying backpressure (no frame drop)")
                continue

        if queue_block_start is not None:
            blocked_for = time.monotonic() - queue_block_start
            log(f"frame queue recovered after {blocked_for:.2f}s")
            queue_block_start = None

    log("Frame reader thread exiting")


# ============================================================
# Motion detection
# ============================================================

# Reuse the morphology kernel instead of allocating one for every detection.
MOTION_KERNEL = cv2.getStructuringElement(
    cv2.MORPH_ELLIPSE,
    (MORPH_KERNEL_SIZE, MORPH_KERNEL_SIZE),
)


def detect_motion(gray1, gray2, gray3):
    diff1 = cv2.absdiff(gray1, gray2)
    diff2 = cv2.absdiff(gray2, gray3)

    _, mask1 = cv2.threshold(
        diff1,
        MOTION_THRESHOLD,
        255,
        cv2.THRESH_BINARY,
    )

    _, mask2 = cv2.threshold(
        diff2,
        MOTION_THRESHOLD,
        255,
        cv2.THRESH_BINARY,
    )

    motion = cv2.bitwise_and(mask1, mask2)

    motion = cv2.morphologyEx(
        motion,
        cv2.MORPH_OPEN,
        MOTION_KERNEL,
        iterations=1,
    )

    if DILATE_ITERATIONS:
        motion = cv2.dilate(
            motion,
            MOTION_KERNEL,
            iterations=DILATE_ITERATIONS,
        )

    contours, _ = cv2.findContours(
        motion,
        cv2.RETR_EXTERNAL,
        cv2.CHAIN_APPROX_SIMPLE,
    )

    boxes = []
    scale_x = WIDTH / DETECT_WIDTH
    scale_y = HEIGHT / DETECT_HEIGHT

    for contour in contours:
        area = cv2.contourArea(contour)
        if area < MIN_MOTION_AREA:
            continue

        x, y, w, h = cv2.boundingRect(contour)
        if w < 5 or h < 5:
            continue

        x1 = max(0, int(x * scale_x))
        y1 = max(0, int(y * scale_y))
        x2 = min(WIDTH - 1, int((x + w) * scale_x))
        y2 = min(HEIGHT - 1, int((y + h) * scale_y))

        boxes.append(
            (x1, y1, x2 - x1, y2 - y1, area)
        )

    boxes.sort(key=lambda item: item[4], reverse=True)
    return boxes[:MAX_MOTION_BOXES]


def merge_boxes(boxes):
    if not boxes:
        return []

    merged = []

    for x, y, w, h, _ in boxes:
        x2 = x + w
        y2 = y + h
        did_merge = False

        for i, (ox, oy, ow, oh) in enumerate(merged):
            ox2 = ox + ow
            oy2 = oy + oh
            p = BOX_MERGE_PADDING

            if not (
                x2 < ox - p or
                x > ox2 + p or
                y2 < oy - p or
                y > oy2 + p
            ):
                nx1 = min(x, ox)
                ny1 = min(y, oy)
                nx2 = max(x2, ox2)
                ny2 = max(y2, oy2)
                merged[i] = (
                    nx1,
                    ny1,
                    nx2 - nx1,
                    ny2 - ny1,
                )
                did_merge = True
                break

        if not did_merge:
            merged.append((x, y, w, h))

    return merged[:MAX_MOTION_BOXES]


# ============================================================
# Drawing
# ============================================================

def put_outlined_text(frame, text, origin, scale, color) -> None:
    """
    Draw text with a TRANSPARENT 1px outline (no background bar).

    OpenCV 5 shifts the text baseline when `thickness` differs, so the
    usual "thick black + thin white" pair renders as two misaligned
    copies of the text. Every putText() here uses thickness=1 and the
    outline is built from same-thickness copies at 1px offsets instead.
    """
    x, y = origin
    font = cv2.FONT_HERSHEY_SIMPLEX
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            if dx == 0 and dy == 0:
                continue
            cv2.putText(
                frame, text, (x + dx, y + dy),
                font, scale, (0, 0, 0), 1, cv2.LINE_AA,
            )
    cv2.putText(
        frame, text, origin,
        font, scale, color, 1, cv2.LINE_AA,
    )


def draw_live_clock(frame, captured_at: float, loop_ms: float = 0.0) -> None:
    """
    Burn the sender wall clock into the frame.

    WARNING: this is the DRAW clock, NOT the CAPTURE clock. A frame that
    was captured 10s ago but only now reaches Python still gets a fresh
    SENT time here. "age" is only the Python queue wait, so neither value
    can detect an upstream (RTSP / FFmpeg / pipe) backlog. loop_ms is the
    PREVIOUS frame's main-loop wall time and CAN expose a stalled sender.
    """
    stamp = datetime.fromtimestamp(
        time.time()
    ).strftime("%H:%M:%S.%f")[:-3]
    age_ms = (time.monotonic() - captured_at) * 1000.0
    text = f"SENT {stamp} age={age_ms:.0f}ms loop={loop_ms:.0f}ms"

    # Transparent background: no bar, just outlined white text.
    put_outlined_text(
        frame,
        text,
        (5, HEIGHT - 6),
        0.40,
        (255, 255, 255),
    )


def draw_boxes(frame, boxes) -> None:
    font_scale = max(0.35, round(WIDTH / 800.0, 2))
    box_thickness = max(1, int(WIDTH / 200.0))

    for idx, (x, y, w, h) in enumerate(boxes, 1):
        x2 = x + w
        y2 = y + h

        cv2.rectangle(
            frame,
            (x, y),
            (x2, y2),
            (0, 255, 0),
            box_thickness,
        )

        cv2.putText(
            frame,
            f"MOTION {idx}",
            (x, max(int(14 * font_scale / 0.4), y - 4)),
            cv2.FONT_HERSHEY_SIMPLEX,
            font_scale,
            (0, 255, 0),
            1,
            cv2.LINE_AA,
        )


def draw_info(frame, brightness, box_count, event_active) -> None:
    """Draw transparent text only; dynamically scales text to resolution."""
    percent = brightness * 100.0 / 255.0

    text1 = f"Brightness: {brightness:.1f} ({percent:.1f}%)"

    status = f"Motion: {box_count}"
    if event_active:
        status += " EVENT"

    scale = max(0.38, round(WIDTH / 750.0, 2))
    line1_y = int(HEIGHT * 0.075)
    line2_y = int(HEIGHT * 0.16)

    # Transparent outlined text (white / yellow), no background bar.
    put_outlined_text(
        frame,
        text1,
        (8, line1_y),
        scale,
        (255, 255, 255),
    )
    put_outlined_text(
        frame,
        status,
        (8, line2_y),
        scale,
        (0, 255, 255),
    )


# ============================================================
# Event segment manager
# ============================================================

@dataclass
class EventSession:
    event_id: int
    start_time: float
    event_dir: Path
    seen_sources: dict
    end_requested: bool = False
    end_time: float = 0.0
    finalize_after: float = 0.0
    next_part: int = 0
    copy_lock: threading.Lock = field(default_factory=threading.Lock, repr=False)


class EventSegmentManager:
    """
    Copies rolling MJPEG segments into per-event directories while motion is
    active. This means an event can last much longer than the rolling window
    without losing its earlier video and without storing frames in Python RAM.
    """

    def __init__(self, rolling_dir: Path, event_root: Path):
        self.rolling_dir = rolling_dir
        self.event_root = event_root
        self.lock = threading.Lock()
        self.sessions = {}
        self.next_id = 1
        self.callback = None

        self.thread = threading.Thread(
            target=self._run,
            name="segment-manager",
            daemon=True,
        )
        self.thread.start()

    def set_callback(self, callback):
        self.callback = callback

    def start_event(self, start_time: float) -> int:
        with self.lock:
            event_id = self.next_id
            self.next_id += 1

            event_dir = self.event_root / (
                f"event_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{event_id:04d}"
            )
            event_dir.mkdir(parents=True, exist_ok=True)

            session = EventSession(
                event_id=event_id,
                start_time=start_time,
                event_dir=event_dir,
                seen_sources={},
                next_part=0,
            )
            self.sessions[event_id] = session

        # Try to copy the pre-event rolling window immediately.
        self._copy_segments_for_session(session)

        log(
            f"event session started: id={event_id}, "
            f"dir={event_dir}"
        )
        return event_id

    def request_finish(self, event_id: int, end_time: float) -> None:
        with self.lock:
            session = self.sessions.get(event_id)
            if session is None:
                return

            session.end_requested = True
            session.end_time = end_time
            # Wait for the current 1-second segment to close.
            session.finalize_after = (
                time.time()
                + SEGMENT_SECONDS
                + 0.25
            )

    def _list_stable_segments(self):
        items = []

        try:
            paths = list(self.rolling_dir.glob("seg_*.mkv"))
        except OSError:
            return items

        now = time.time()

        for path in paths:
            try:
                st = path.stat()
            except OSError:
                continue

            # A segment newer than this is likely still being written.
            if now - st.st_mtime < 0.15:
                continue

            if st.st_size <= 0:
                continue

            items.append((st.st_mtime, path))

        items.sort(key=lambda item: item[0])
        return items

    def _copy_segments_for_session(self, session: EventSession):
        with session.copy_lock:
            self._copy_segments_for_session_locked(session)

    def _copy_segments_for_session_locked(self, session: EventSession):
        segments = self._list_stable_segments()

        for mtime, src in segments:
            # Include the pre-event rolling window, then every later segment.
            if mtime < session.start_time - PRE_EVENT_SECONDS - SEGMENT_SECONDS:
                continue

            try:
                st = src.stat()
            except OSError:
                continue

            signature = (st.st_mtime_ns, st.st_size)
            source_key = src.name

            # A wrapped file name may be reused. Copy it again when its
            # timestamp/size changes.
            if session.seen_sources.get(source_key) == signature:
                continue

            dst = session.event_dir / (
                f"part_{session.next_part:05d}.mkv"
            )

            try:
                shutil.copyfile(src, dst)
            except OSError:
                # The rolling file may be replaced while copying. Retry later.
                continue

            session.seen_sources[source_key] = signature
            session.next_part += 1

    def _finish_ready_sessions(self):
        ready = []
        now = time.time()

        with self.lock:
            for event_id, session in list(self.sessions.items()):
                if not session.end_requested:
                    continue

                # Capture any final closed rolling segment first.
                self._copy_segments_for_session(session)

                if now < session.finalize_after:
                    continue

                try:
                    files = sorted(
                        session.event_dir.glob("part_*.mkv"),
                        key=lambda p: p.name,
                    )
                except OSError:
                    files = []

                self.sessions.pop(event_id, None)
                ready.append(
                    (
                        session,
                        files,
                    )
                )

        for session, files in ready:
            if self.callback is not None:
                self.callback(
                    session,
                    files,
                )

    def _run(self):
        while not STOP_EVENT.is_set():
            with self.lock:
                sessions = list(self.sessions.values())

            for session in sessions:
                self._copy_segments_for_session(session)

            self._finish_ready_sessions()
            time.sleep(0.20)

    def close(self):
        # Manager is daemonized; main process shutdown will stop it.
        pass


def cleanup_event_dir(event_dir: Path) -> None:
    try:
        shutil.rmtree(event_dir)
    except OSError as exc:
        log(f"failed to remove event dir: {exc}")


# ============================================================
# Event MP4 encoding
# ============================================================

def encode_event_mp4(
    event_dir: Path,
    segment_files,
    output_path: str,
) -> bool:
    if not segment_files:
        log("event has no segments")
        return False

    concat_file = event_dir / "concat.txt"

    try:
        # Files are already copied out of the rolling directory, so wrapping
        # cannot modify them while the encoder reads them.
        # They are Matroska segments carrying MJPEG video.
        ordered = sorted(
            segment_files,
            key=lambda p: p.name,
        )

        with concat_file.open("w", encoding="ascii") as fp:
            for path in ordered:
                fp.write(
                    f"file '{path.as_posix()}'\n"
                )

        command = [
            "ffmpeg",
            "-hide_banner",
            "-nostdin",
            "-loglevel", "warning",
            "-f", "concat",
            "-safe", "0",
            "-i", str(concat_file),
            "-an",
            "-c:v", EVENT_H264_ENCODER,
            "-pix_fmt", "yuv420p",
            "-b:v", EVENT_VIDEO_BITRATE,
            "-movflags", "+faststart",
            "-y", output_path,
        ]

        if EVENT_H264_ENCODER == "libx264":
            command[command.index("-c:v") + 2:command.index("-c:v") + 2] = [
                "-preset", EVENT_X264_PRESET,
                "-tune", "zerolatency",
            ]

        if DEBUG:
            log("event ffmpeg: " + " ".join(command))

        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        stderr = process.stderr.read() if process.stderr else b""
        rc = process.wait()

        if rc == 0 and os.path.isfile(output_path):
            size = os.path.getsize(output_path)
            if size > 1000:
                log(
                    f"event mp4 ready: "
                    f"{size / (1024 * 1024):.2f} MB"
                )
                return True

        if stderr:
            text = stderr.decode(
                "utf-8",
                errors="ignore",
            ).strip()
            if text:
                log(
                    f"event ffmpeg: {text[:1500]}"
                )

        return False

    except Exception as exc:
        log(
            f"event mp4 encode failed: "
            f"{type(exc).__name__}: {exc}"
        )
        return False


def process_finished_event(
    session: EventSession,
    segment_files,
    telegram_worker: TelegramWorker,
    motion_count: int,
    publisher: Optional[MediaPublisher],
) -> None:
    event_dir = session.event_dir

    fd, mp4_path = tempfile.mkstemp(
        prefix="motion_",
        suffix=".mp4",
    )
    os.close(fd)

    try:
        ok = encode_event_mp4(
            event_dir,
            segment_files,
            mp4_path,
        )

        if not ok:
            try:
                os.remove(mp4_path)
            except OSError:
                pass
            return

        duration = max(
            0.0,
            session.end_time - session.start_time,
        )

        start_text = datetime.fromtimestamp(
            session.start_time
        ).strftime("%Y-%m-%d %H:%M:%S")

        # Publish BEFORE the Telegram worker takes ownership: it DELETES
        # the temp MP4 as soon as the upload finishes. Copying afterwards
        # would race with that delete.
        public_url = None
        if publisher is not None:
            try:
                public_url = publisher.publish_event_mp4(
                    mp4_path,
                    session,
                    motion_count,
                )
            except Exception as exc:
                log(
                    f"publish event mp4 failed: "
                    f"{type(exc).__name__}: {exc}"
                )

        caption = (
            "Motion event\n"
            f"Time: {start_text}\n"
            f"Duration: about {duration:.1f}s\n"
            f"Motion regions: {motion_count}\n"
            f"Video: {WIDTH}x{HEIGHT}@{FPS}"
        )
        if public_url:
            caption += f"\nReplay: {public_url}"

        if telegram_worker.submit(
            mp4_path,
            caption,
        ):
            mp4_path = None

    finally:
        cleanup_event_dir(event_dir)

        if mp4_path:
            try:
                os.remove(mp4_path)
            except OSError:
                pass


# ============================================================
# Main
# ============================================================

def main():
    live_queue_size = (
        LIVE_QUEUE_SIZE if LOW_LATENCY else FRAME_QUEUE_SIZE
    )
    # Push cap applies only BELOW the input frame rate. A cap >= INPUT_FPS
    # would gate frames on a grid as tight as the frame grid itself, where
    # ordinary arrival jitter makes the check randomly fail (at 24/24 the
    # measured tx wobbles around ~23fps instead of a clean 24). Treat it
    # as uncapped instead; tx then simply equals INPUT_FPS.
    tx_interval = (
        1.0 / LIVE_OUTPUT_FPS
        if 0 < LIVE_OUTPUT_FPS < INPUT_FPS
        else 0.0
    )
    next_tx_at = 0.0

    log("ARM64 RTSP motion monitor V2.1 starting (low-latency live)")
    log(f"output: {WIDTH}x{HEIGHT}@{FPS} (capture {INPUT_FPS}fps)")
    if LOW_LATENCY:
        log(
            f"frame queue: {live_queue_size} frame(s), "
            f"latest-frame / drop-stale enabled"
        )
        log("live pacing: -re disabled, mux buffering disabled")
        log(
            "live timeline: wall-clock PTS (no drift)"
            if LIVE_WALLCLOCK_PTS
            else "live timeline: frame-count PTS (may drift)"
        )
    else:
        log(
            f"frame queue: {live_queue_size} frames, "
            f"legacy no-drop backpressure enabled"
        )
    if LIVE_CLOCK_OVERLAY:
        log(
            "diagnostic: SENT wall clock overlaid at bottom; "
            "compare it with a phone clock to measure real lag"
        )
    if UDP_VIDEO_URL_EXTRA:
        log(f"mirror UDP target: {UDP_VIDEO_URL_EXTRA}")
    if LIVE_OUTPUT_FPS > 0 and tx_interval > 0:
        log(
            f"push rate cap: {LIVE_OUTPUT_FPS:g} fps "
            f"(detector still runs at {INPUT_FPS})"
        )
    elif LIVE_OUTPUT_FPS > 0:
        log(
            f"push rate cap: {LIVE_OUTPUT_FPS:g} >= INPUT_FPS "
            f"{INPUT_FPS} -> uncapped, every frame is sent"
        )
    else:
        log("push rate cap: disabled (LIVE_OUTPUT_FPS=0)")
    log(
        f"upstream probe: healthy pipe_wait is ~{1000.0 / max(1, INPUT_FPS):.0f}ms "
        f"(pipe empty); ~0ms means backlog is growing"
    )
    if LIVE_OUTPUT_FPS > 0 and INPUT_FPS > 2 * LIVE_OUTPUT_FPS:
        log(
            f"WARNING: INPUT_FPS={INPUT_FPS} but push cap is "
            f"{LIVE_OUTPUT_FPS:g}fps; ~{(1 - LIVE_OUTPUT_FPS / INPUT_FPS) * 100:.0f}% "
            f"of decoded frames cross the rawvideo pipe "
            f"({WIDTH * HEIGHT * 3 * INPUT_FPS / 1e6:.1f} MB/s) only to be "
            f"discarded. If pipe_wait stays near 0ms, set INPUT_FPS=12 in .env"
        )
    log(
        f"motion: {DETECT_WIDTH}x{DETECT_HEIGHT}, "
        f"every {DETECT_EVERY_N_FRAMES} frames"
    )
    log(
        f"event: pre={PRE_EVENT_SECONDS}s, "
        f"post={POST_EVENT_SECONDS}s, "
        f"max={MAX_EVENT_SECONDS}s"
    )
    log(
        f"mjpeg encoder: {MJPEG_ENCODER}, q={MJPEG_QUALITY}"
    )
    log(
        f"event h264 encoder: {EVENT_H264_ENCODER}"
    )
    log(
        f"telegram timeouts: connect={TELEGRAM_CONNECT_TIMEOUT}s "
        f"read={TELEGRAM_READ_TIMEOUT}s retries={TELEGRAM_MAX_RETRIES}"
    )
    if SNAPSHOT_NOTIFY:
        log(
            f"snapshot notify: on, min interval="
            f"{SNAPSHOT_MIN_INTERVAL:.0f}s, "
            f"quiet hours={NOTIFY_QUIET_HOURS or 'none'}"
        )

    publisher = None
    hls_publisher = None
    hls_active = False
    if PUBLISH_ENABLED:
        publisher = MediaPublisher(PUBLISH_DIR)
        hls_publisher = HlsPublisher(publisher.hls_dir)
        log(f"cdn origin dir: {publisher.root}")
        if PUBLIC_BASE_URL:
            log(f"cdn base url: {PUBLIC_BASE_URL}")
        log(
            "cdn urls: hls/stream.m3u8 (live), "
            "stream.mp4 (latest event / no-cache), "
            "events/*.mp4 (immutable), latest.json, latest.jpg"
        )
        if HLS_ENABLED:
            log(
                f"hls: 1 RTSP session, queue={HLS_QUEUE_SIZE} frame(s), "
                f"wall-clock PTS, {HLS_FPS}fps nominal, "
                f"segment={HLS_TIME:g}s x {HLS_LIST_SIZE}, "
                f"encoder={HLS_H264_ENCODER}"
            )
            hls_publisher.start()
            hls_active = True
        else:
            log("hls: disabled (HLS_ENABLED=0)")
    else:
        log("cdn publishing: disabled (PUBLISH_ENABLED=0)")

    rolling_dir = ensure_dir(ROLLING_SEGMENT_DIR)
    event_root = ensure_dir(EVENT_SEGMENT_DIR)
    cleanup_rolling_dir(rolling_dir)

    telegram = TelegramClient(
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        TELEGRAM_SOCKS5,
    )

    try:
        telegram.check_socks_support()
    except RuntimeError as exc:
        log(f"FATAL: {exc}")
        return

    if not telegram.test_proxy():
        log(
            "WARNING: Telegram SOCKS5 proxy test failed. "
            "Uploads will likely fail until the proxy is fixed. "
            "Check TELEGRAM_SOCKS5, network reachability, and proxy auth."
        )
        # Continue anyway - the monitor itself is still useful without Telegram.

    telegram_worker = TelegramWorker(telegram)
    snapshot_notifier = SnapshotNotifier(
        telegram_worker,
        publisher,
    )

    # Input FFmpeg is deliberately restartable. The output FFmpeg stays alive
    # so the UDP stream and rolling segments continue across RTSP reconnects.
    decoder = start_input_ffmpeg()
    encoder = start_output_ffmpeg(rolling_dir)
    frame_clock = FrameClock()
    live_stats = LiveStats()

    def start_decoder_reader(current_decoder):
        reader_stop_event = threading.Event()
        # Give the fresh process a full stall window to connect and start
        # delivering frames before the watchdog can consider it stalled.
        frame_clock.touch()
        thread = threading.Thread(
            target=frame_reader_worker,
            args=(
                current_decoder,
                frame_size,
                frame_queue,
                STOP_EVENT,
                reader_stop_event,
                frame_clock,
                live_stats,
                LOW_LATENCY,
            ),
            name="frame-reader",
            daemon=True,
        )
        thread.start()
        return thread, reader_stop_event

    threading.Thread(
        target=stderr_reader,
        args=(encoder, "OUTPUT"),
        daemon=True,
    ).start()

    # Event manager callback is bound after the manager exists.
    event_manager = EventSegmentManager(
        rolling_dir,
        event_root,
    )

    # Main-thread event state.
    event_active = False
    event_id = None
    event_start_time = 0.0
    last_motion_time = 0.0
    event_motion_count = 0
    event_count_lock = threading.Lock()
    event_count_by_id = {}

    def event_ready_callback(session, files):
        with event_count_lock:
            motion_count = event_count_by_id.pop(
                session.event_id,
                0,
            )

        threading.Thread(
            target=process_finished_event,
            args=(
                session,
                files,
                telegram_worker,
                motion_count,
                publisher,
            ),
            name=f"event-worker-{session.event_id}",
            daemon=True,
        ).start()

    event_manager.set_callback(
        event_ready_callback
    )

    prev_prev_gray = None
    prev_gray = None
    current_boxes = []
    last_brightness = 0.0

    frame_index = 0
    motion_confirm_count = 0
    frame_size = WIDTH * HEIGHT * 3
    frame_queue = queue.Queue(maxsize=live_queue_size)

    # Start stderr logging for the first decoder.
    threading.Thread(
        target=stderr_reader,
        args=(decoder, "INPUT"),
        daemon=True,
    ).start()

    # Start the first reader after frame_size/frame_queue exist.
    reader_thread, reader_stop_event = start_decoder_reader(decoder)

    # Prevent an input reconnect from hammering the RTSP server.
    reconnect_delay = RTSP_RECONNECT_DELAY

    stat_start = time.monotonic()
    stat_frames = 0
    stat_tx = 0
    stat_age_sum = 0.0
    stat_age_max = 0.0
    stat_loop_sum = 0.0
    stat_loop_max = 0.0
    prev_loop_ms = 0.0
    last_stall_restart = 0.0
    total_frames = 0

    try:
        while not STOP_EVENT.is_set():
            if encoder.poll() is not None:
                log("output ffmpeg exited")
                break

            # RTSP input may legitimately disappear temporarily. Reconnect
            # only the input side; do not stop the monitor/output pipeline.
            #
            # The stalled() check is what actually fixes "no auto-recovery
            # after the upstream RTSP restarts": in that scenario FFmpeg's
            # process can stay running and the reader thread can stay
            # "alive" (blocked in a blocking read()) forever, so neither of
            # the other two conditions would ever fire on their own.
            is_stalled = frame_clock.stalled(RTSP_STALL_TIMEOUT)
            if decoder.poll() is not None or not reader_thread.is_alive() or is_stalled:
                if is_stalled and decoder.poll() is None:
                    log(
                        f"input ffmpeg produced no data for "
                        f"{RTSP_STALL_TIMEOUT:.0f}s (likely stalled after "
                        f"upstream restart); forcing reconnect"
                    )
                else:
                    log("input ffmpeg/reader stopped; reconnecting RTSP")

                reader_stop_event.set()
                try:
                    reader_thread.join(timeout=1.0)
                except Exception:
                    pass

                try:
                    if decoder.poll() is None:
                        decoder.terminate()
                        try:
                            decoder.wait(timeout=2)
                        except subprocess.TimeoutExpired:
                            decoder.kill()
                except Exception:
                    pass

                if STOP_EVENT.wait(reconnect_delay):
                    break

                reconnect_delay = min(
                    reconnect_delay * 1.5,
                    RTSP_RECONNECT_MAX_DELAY,
                )

                try:
                    decoder = start_input_ffmpeg()

                    threading.Thread(
                        target=stderr_reader,
                        args=(decoder, "INPUT"),
                        daemon=True,
                    ).start()

                    reader_thread, reader_stop_event = start_decoder_reader(decoder)

                    # A successful reconnect should immediately return the
                    # retry interval to its normal value.
                    reconnect_delay = RTSP_RECONNECT_DELAY
                    log("RTSP input reconnected")
                except Exception as exc:
                    log(
                        f"RTSP reconnect failed: "
                        f"{type(exc).__name__}: {exc}"
                    )
                    continue

            try:
                raw, captured_at = frame_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            loop_start = time.monotonic()
            frame_age_ms = (time.monotonic() - captured_at) * 1000.0
            stat_age_sum += frame_age_ms
            stat_age_max = max(stat_age_max, frame_age_ms)

            frame_view = np.frombuffer(
                raw,
                dtype=np.uint8,
            ).reshape((HEIGHT, WIDTH, 3))
            # raw is a WRITABLE bytearray now, so this view is writable:
            # motion detection / brightness below only READ it, then the
            # overlay for transmitted frames is drawn IN PLACE and the
            # encoder gets a flat zero-copy view of the same buffer.
            frame = frame_view

            # Decide FIRST whether this frame will be transmitted. With the
            # 24/24 config every frame is pushed; with a lower cap (e.g.
            # LIVE_OUTPUT_FPS=6 at INPUT_FPS=24) ~75% are dropped. Skipping
            # frame.copy() + 27 putText() calls on dropped frames is a real
            # but small saving (~0.163ms/frame measured), and it also keeps
            # detection sensitivity unchanged at full INPUT_FPS.
            push_frame = True
            if tx_interval:
                tx_now = time.monotonic()
                if tx_now < next_tx_at:
                    push_frame = False
                else:
                    # Absolute grid: advance exactly ONE slot per push, so
                    # the average rate equals the cap no matter how the
                    # frame arrival jitters. (The old relative anchor
                    # "next = push_time + interval" lost a slot whenever a
                    # frame arrived just before the grid, which is why a
                    # 6fps cap once measured 5.00fps in the tx= log.)
                    next_tx_at += tx_interval
                    if next_tx_at < tx_now:
                        # A stall left the grid behind real time; re-anchor
                        # to now instead of bursting through missed slots.
                        next_tx_at = tx_now + tx_interval

            now = time.time()

            # ----------------------------------------------------
            # Motion detection every N frames on 160x120.
            # ----------------------------------------------------
            if frame_index % DETECT_EVERY_N_FRAMES == 0:
                gray_small = cv2.cvtColor(
                    frame_view,
                    cv2.COLOR_BGR2GRAY,
                )
                gray_small = cv2.resize(
                    gray_small,
                    (DETECT_WIDTH, DETECT_HEIGHT),
                    interpolation=cv2.INTER_AREA,
                )
                gray_small = cv2.GaussianBlur(
                    gray_small,
                    (3, 3),
                    0,
                )

                if prev_prev_gray is not None and prev_gray is not None:
                    raw_boxes = detect_motion(
                        prev_prev_gray,
                        prev_gray,
                        gray_small,
                    )
                    current_boxes = merge_boxes(raw_boxes)
                else:
                    current_boxes = []

                prev_prev_gray = prev_gray
                prev_gray = gray_small

                if current_boxes:
                    motion_confirm_count += 1
                else:
                    motion_confirm_count = 0
            else:
                # No motion computation on intermediate frames.
                # Keep the most recent detection result for stable overlays.
                current_boxes = current_boxes

            confirmed_motion = (
                motion_confirm_count
                >= MOTION_CONFIRM_FRAMES
            )

            # ----------------------------------------------------
            # Brightness. Computed on detection frames from the
            # 160x120 gray image and simply REUSED on the others:
            # the old strided float mean allocated 3 temporaries on
            # EVERY non-detect frame for a purely cosmetic number.
            # ----------------------------------------------------
            if frame_index % DETECT_EVERY_N_FRAMES == 0:
                brightness = float(
                    np.mean(gray_small)
                )
                last_brightness = brightness
            else:
                brightness = last_brightness

            # ----------------------------------------------------
            # Event state (does not need a writable frame).
            # ----------------------------------------------------
            event_started_now = False
            if confirmed_motion:
                if not event_active:
                    event_active = True
                    event_start_time = max(
                        0.0,
                        now - PRE_EVENT_SECONDS,
                    )
                    last_motion_time = now
                    event_motion_count = len(current_boxes)

                    event_id = event_manager.start_event(
                        event_start_time
                    )

                    with event_count_lock:
                        event_count_by_id[
                            event_id
                        ] = event_motion_count

                    log(
                        f"motion event start: id={event_id}"
                    )
                    event_started_now = True
                else:
                    last_motion_time = now
                    event_motion_count = max(
                        event_motion_count,
                        len(current_boxes),
                    )

                    if event_id is not None:
                        with event_count_lock:
                            event_count_by_id[
                                event_id
                            ] = event_motion_count

            # ----------------------------------------------------
            # Overlay ONLY for frames that are sent (or for the one
            # frame an event-start snapshot needs). raw is writable,
            # so the overlay is drawn IN PLACE - no full-frame copy.
            # put_outlined_text() draws each line 9 times (8 neighbours
            # + foreground), so with a push cap below INPUT_FPS this
            # also skips all putText work on dropped frames.
            # ----------------------------------------------------
            need_annotate = push_frame or event_started_now
            if need_annotate:
                frame = frame_view
                draw_boxes(frame, current_boxes)
                draw_info(
                    frame,
                    brightness,
                    len(current_boxes),
                    event_active,
                )
                if LIVE_CLOCK_OVERLAY:
                    draw_live_clock(
                        frame, captured_at, prev_loop_ms
                    )

            # Instant Telegram snapshot on the FULLY annotated frame.
            # Runs once per event, never in the per-frame live path.
            if event_started_now:
                snapshot_notifier.on_event_start(
                    frame,
                    event_id,
                    brightness,
                    len(current_boxes),
                )

            # ----------------------------------------------------
            # One raw BGR write. FFmpeg performs the only MJPEG
            # encode and also writes the UDP live stream.
            #
            # Push-rate cap: motion detection still runs at full FPS;
            # only the copy / overlay / write are skipped on dropped
            # frames, so detection sensitivity is unchanged.
            # ----------------------------------------------------
            if push_frame:
                try:
                    # ONE flat view shared by both consumers: the UDP
                    # encoder and the HLS queue. Zero-copy end to end -
                    # the annotated array stays alive until both have
                    # written it (the next frame uses a fresh buffer).
                    payload = memoryview(frame_view).cast("B")
                    encoder.stdin.write(payload)
                    # stdin is a BufferedWriter (bufsize=1MB). Without
                    # this flush it can hold up to 4 full BGR frames
                    # before handing anything to FFmpeg.
                    encoder.stdin.flush()
                    stat_tx += 1

                    # Non-blocking handoff. If the HLS writer is stuck on
                    # a slow disk the queue just replaces its old frame;
                    # nothing on this thread ever waits for HLS.
                    if hls_active:
                        hls_publisher.submit(payload)
                except (BrokenPipeError, OSError) as exc:
                    log(
                        f"output ffmpeg pipe failed: {exc}"
                    )
                    break

            # ----------------------------------------------------
            # Event end decision. The manager will wait for the
            # current rolling segment to close before finalizing.
            # ----------------------------------------------------
            if event_active:
                if (
                    now - event_start_time
                    >= MAX_EVENT_SECONDS
                ):
                    end_time = now
                    log(
                        f"max event duration reached: id={event_id}"
                    )

                    if event_id is not None:
                        event_manager.request_finish(
                            event_id,
                            end_time,
                        )

                    event_active = False
                    event_start_time = 0.0
                    last_motion_time = 0.0
                    event_motion_count = 0
                    event_id = None

                elif (
                    last_motion_time > 0
                    and now - last_motion_time
                    >= POST_EVENT_SECONDS
                ):
                    end_time = now

                    log(
                        f"motion event end: id={event_id}"
                    )

                    if event_id is not None:
                        event_manager.request_finish(
                            event_id,
                            end_time,
                        )

                    event_active = False
                    event_start_time = 0.0
                    last_motion_time = 0.0
                    event_motion_count = 0
                    event_id = None

            # ----------------------------------------------------
            # Optional preview.
            # ----------------------------------------------------
            if DISPLAY:
                # frame_view is writable now, so imshow() accepts it
                # whether or not this frame was annotated.
                cv2.imshow("RTSP Motion", frame_view)
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord("q")):
                    STOP_EVENT.set()
                    break

            prev_loop_ms = (time.monotonic() - loop_start) * 1000.0
            stat_loop_sum += prev_loop_ms
            stat_loop_max = max(stat_loop_max, prev_loop_ms)

            frame_index += 1
            total_frames += 1
            stat_frames += 1

            elapsed = time.monotonic() - stat_start
            if elapsed >= 5.0:
                avg_age = stat_age_sum / max(1, stat_frames)
                avg_loop = stat_loop_sum / max(1, stat_frames)
                dropped = live_stats.take_dropped()
                pipe_wait, drain_pct = live_stats.take_pipe()

                # A stalled main loop means the sender is not keeping up.
                # Reconnecting the RTSP input discards every buffered frame
                # upstream of Python, which no queue tuning can do.
                if (
                    INPUT_STALL_RESTART
                    and stat_loop_max >= LOOP_STALL_RESTART_MS
                    and time.monotonic() - last_stall_restart
                    >= LOOP_STALL_COOLDOWN_S
                ):
                    last_stall_restart = time.monotonic()
                    log(
                        f"main loop stalled {stat_loop_max:.0f}ms; "
                        f"forcing RTSP reconnect to flush upstream backlog"
                    )
                    try:
                        if decoder.poll() is None:
                            decoder.terminate()
                    except Exception:
                        pass

                # Upstream pipe backlog is invisible to age/lag/loop metrics.
                # When read_exact() keeps returning instantly, the OS pipe is
                # never empty and FFmpeg (plus the RTSP socket buffers behind
                # it) is replaying frames produced seconds ago. No downstream
                # queue tuning can flush those bytes; only reconnecting the
                # RTSP input discards them and pulls the picture back to now.
                if (
                    PIPE_BACKLOG_RESTART
                    and stat_frames >= INPUT_FPS * 2
                    and pipe_wait < PIPE_BACKLOG_MS
                    and drain_pct > 80.0
                    and time.monotonic() - last_stall_restart
                    >= LOOP_STALL_COOLDOWN_S
                ):
                    last_stall_restart = time.monotonic()
                    log(
                        f"upstream pipe full (wait={pipe_wait:.0f}ms, "
                        f"instant reads={drain_pct:.0f}%); forcing RTSP "
                        f"reconnect to flush buffered frames"
                    )
                    try:
                        if decoder.poll() is None:
                            decoder.terminate()
                    except Exception:
                        pass

                log(
                    f"fps={stat_frames / elapsed:.2f} "
                    f"tx={stat_tx / elapsed:.2f} "
                    f"infps={INPUT_FPS} "
                    f"pipe_wait={pipe_wait:.0f}ms "
                    f"drain={drain_pct:.0f}% "
                    f"dropped={dropped} "
                    f"hlsq={hls_publisher.take_dropped() if hls_active else 0} "
                    f"lag_avg={avg_age:.0f}ms "
                    f"lag_max={stat_age_max:.0f}ms "
                    f"loop_avg={avg_loop:.0f}ms "
                    f"loop_max={stat_loop_max:.0f}ms "
                    f"motion={len(current_boxes)} "
                    f"event={event_active}"
                )
                stat_frames = 0
                stat_tx = 0
                stat_age_sum = 0.0
                stat_age_max = 0.0
                stat_loop_sum = 0.0
                stat_loop_max = 0.0
                stat_start = time.monotonic()

    except KeyboardInterrupt:
        STOP_EVENT.set()

    except Exception as exc:
        log(
            f"main loop error: "
            f"{type(exc).__name__}: {exc}"
        )

    finally:
        log("stopping")
        STOP_EVENT.set()

        # Stop the independent HLS process first so it cannot leave a
        # "live" playlist pointing at a publisher that no longer exists.
        if hls_publisher is not None:
            try:
                hls_publisher.close()
            except Exception as exc:
                log(
                    f"hls publisher stop error: "
                    f"{type(exc).__name__}: {exc}"
                )

        # If an event is active, ask the manager to finish what it has.
        if event_active and event_id is not None:
            try:
                event_manager.request_finish(
                    event_id,
                    time.time(),
                )
            except Exception:
                pass

        try:
            reader_stop_event.set()
        except Exception:
            pass

        try:
            if reader_thread.is_alive():
                reader_thread.join(timeout=1.0)
        except Exception:
            pass

        try:
            if decoder.stdout:
                decoder.stdout.close()
        except Exception:
            pass

        try:
            if decoder.poll() is None:
                decoder.terminate()
                try:
                    decoder.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    decoder.kill()
        except Exception:
            pass

        try:
            if encoder.stdin:
                encoder.stdin.close()
        except Exception:
            pass

        try:
            if encoder.poll() is None:
                encoder.terminate()
                try:
                    encoder.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    encoder.kill()
        except Exception:
            pass

        if DISPLAY:
            try:
                cv2.destroyAllWindows()
            except Exception:
                pass

        deadline = time.monotonic() + 10.0
        while (
            not telegram_worker.jobs.empty()
            and time.monotonic() < deadline
        ):
            time.sleep(0.2)

        log(f"total frames: {total_frames}")
        log("exited")


if __name__ == "__main__":
    main()
