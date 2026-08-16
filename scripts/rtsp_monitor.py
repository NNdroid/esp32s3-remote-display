#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Low-CPU ARM64 RTSP motion monitor.

Live pipeline:
    RTSP -> FFmpeg -> raw BGR -> Python/OpenCV
          -> one FFmpeg MJPEG encoder
               -> UDP MPEG-TS :8888 (same format as reference command)
               -> rolling 1-second Matroska/MJPEG segments

Audio pipeline:
    RTSP -> FFmpeg -> PCM s16le -> UDP :8889

Motion:
    3-frame difference on 160x120, every 2 frames.
    Output remains 320x240 @ 25 FPS.

Event recording:
    - about 5s before first confirmed motion
    - until 10s after the last confirmed motion
    - rolling Matroska/MJPEG files are copied into a per-event directory while
      the event is running, so RAM usage does not grow with event duration
    - after the event, FFmpeg encodes the Matroska/MJPEG parts to H.264 MP4
    - MP4 is uploaded to Telegram through SOCKS5

Important:
    The live Python path does NOT call cv2.imencode().
    MJPEG encoding is performed once by FFmpeg.

All source/log strings are ASCII-safe.
"""

import os
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

import cv2
import numpy as np
import requests

# Load .env file from the same directory if it exists
_env_path = Path(__file__).parent / ".env"
if _env_path.is_file():
    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=_env_path)
    except ImportError:
        # Fallback manual parser if python-dotenv is not installed
        with open(_env_path, "r", encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip()
                if not _line or _line.startswith("#"):
                    continue
                if "=" in _line:
                    _k, _v = _line.split("=", 1)
                    _k = _k.strip()
                    _v = _v.strip().strip("'\"")
                    os.environ.setdefault(_k, _v)

# ============================================================
# Configuration
# ============================================================

RTSP_URL = os.getenv(
    "RTSP_URL",
    "rtsp://192.168.100.2:8554/cam0-outdoor-052c",
)

UDP_VIDEO_URL = os.getenv(
    "UDP_VIDEO_URL",
    "udp://192.168.192.233:8888?pkt_size=1316",
)

UDP_AUDIO_URL = os.getenv(
    "UDP_AUDIO_URL",
    "udp://192.168.192.233:8889?pkt_size=1024",
)

WIDTH = int(os.getenv("WIDTH", "320"))
HEIGHT = int(os.getenv("HEIGHT", "240"))
FPS = int(os.getenv("FPS", "25"))

# Queue buffer size for zero-latency frame caching (prevents stuttering & frame drops)
FRAME_QUEUE_SIZE = int(os.getenv("FRAME_QUEUE_SIZE", "25"))

# RTSP reconnect behavior.
# When the camera/server closes the RTSP connection, the input FFmpeg is
# restarted automatically instead of terminating the whole monitor.
RTSP_RECONNECT_DELAY = float(os.getenv("RTSP_RECONNECT_DELAY", "2.0"))
RTSP_RECONNECT_MAX_DELAY = float(os.getenv("RTSP_RECONNECT_MAX_DELAY", "15.0"))

# Bounded queue with backpressure:
# - no intentional frame dropping
# - FFmpeg blocks when Python falls behind
# - keeps latency bounded instead of allowing an unlimited RAM backlog
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
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "1231231231321:1231231231231231312312",
)
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    "-1231231231231",
)
TELEGRAM_SOCKS5 = os.getenv(
    "TELEGRAM_SOCKS5",
    "socks5h://guest:guest@192.168.100.3:7890",
)
TELEGRAM_QUEUE_SIZE = 2

DISPLAY = False
DEBUG = True


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
# Telegram
# ============================================================

class TelegramClient:
    def __init__(self, bot_token: str, chat_id: str, socks5_url: str):
        self.bot_token = bot_token.strip()
        self.chat_id = chat_id.strip()
        self.socks5_url = socks5_url.strip()
        self.session = requests.Session()
        self.session.trust_env = False

        if self.socks5_url:
            self.session.proxies.update({
                "http": self.socks5_url,
                "https": self.socks5_url,
            })
            log("telegram socks5 configured")
        else:
            log("warning: telegram socks5 is not configured")

        self.base_url = f"https://api.telegram.org/bot{self.bot_token}"

    def request(self, method: str, endpoint: str, **kwargs) -> Optional[dict]:
        if not self.bot_token or not self.chat_id:
            return None

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        try:
            response = self.session.request(
                method=method,
                url=url,
                **kwargs,
            )
            response.raise_for_status()
            result = response.json()
            if not result.get("ok"):
                log(f"telegram api error: {result}")
                return None
            return result
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
                    timeout=(15, 300),
                )

            ok = result is not None
            log(
                "telegram upload success"
                if ok else
                "telegram upload failed"
            )
            return ok

        except Exception as exc:
            log(
                f"telegram upload exception: "
                f"{type(exc).__name__}: {exc}"
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

    def submit(self, video_path: str, caption: str) -> bool:
        try:
            self.jobs.put_nowait((video_path, caption))
            return True
        except queue.Full:
            log("telegram queue full; dropping event upload")
            return False

    def _run(self):
        while not STOP_EVENT.is_set() or not self.jobs.empty():
            try:
                video_path, caption = self.jobs.get(timeout=0.5)
            except queue.Empty:
                continue

            try:
                self.client.send_video(video_path, caption)
            finally:
                try:
                    os.remove(video_path)
                except FileNotFoundError:
                    pass
                except OSError as exc:
                    log(f"failed to remove mp4: {exc}")
                self.jobs.task_done()


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


def start_input_ffmpeg() -> subprocess.Popen:
    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-rtsp_transport", "tcp",
        "-fflags", "+nobuffer+discardcorrupt+genpts",
        "-max_delay", "0",
        "-flags", "low_delay",
        "-use_wallclock_as_timestamps", "1",
        "-probesize", "50000",
        "-analyzeduration", "100000",
        "-i", RTSP_URL,

        # Video -> raw BGR.
        "-map", "0:v:0",
        "-vf", f"fps={FPS},scale={WIDTH}:{HEIGHT}:flags=fast_bilinear",
        "-pix_fmt", "bgr24",
        "-f", "rawvideo",
        "pipe:1",

        # Audio -> PCM -> UDP 8889.
        "-map", "0:a:0?",
        "-af", "aresample=async=1",
        "-c:a", "pcm_s16le",
        "-ar", "44100",
        "-ac", "2",
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
    # The live UDP branch intentionally matches the user's known-good
    # reference command:
    #
    #   -c:v mjpeg -q:v 3 -pix_fmt yuvj420p
    #   -f mpegts udp://...:8888?pkt_size=1316
    #
    # Do NOT change the live UDP branch to H.264. The receiver expects this
    # exact MJPEG-in-MPEGTS behavior. The rolling event branch is intentionally
    # Matroska because FFmpeg can read MJPEG from MKV as a normal video stream.
    # This avoids the MPEG-TS/MJPEG private-data issue when building MP4.
    tee_output = (
        f"[f=mpegts]'{UDP_VIDEO_URL}'"
        f"|[f=segment:segment_time={SEGMENT_SECONDS}"
        f":segment_wrap={SEGMENT_COUNT}"
        f":reset_timestamps=1"
        f":segment_format=matroska]'{pattern}'"
    )

    command = [
        "ffmpeg",
        "-hide_banner",
        "-nostdin",
        "-loglevel", "warning",

        # Python sends annotated BGR frames.
        "-f", "rawvideo",
        "-pix_fmt", "bgr24",
        "-video_size", f"{WIDTH}x{HEIGHT}",
        "-framerate", str(FPS),
        "-re",
        "-i", "pipe:0",

        "-map", "0:v",
        "-an",
        "-c:v", MJPEG_ENCODER,
        "-q:v", str(MJPEG_QUALITY),
        "-pix_fmt", "yuvj420p",

        # Same muxing parameters as the reference command.
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


def frame_reader_worker(
    decoder,
    frame_size: int,
    frame_queue: queue.Queue,
    stop_event: threading.Event,
    reader_stop_event: threading.Event,
) -> None:
    """
    Dedicated producer thread.

    IMPORTANT:
    This version never intentionally discards an already-read frame.
    When the bounded queue is full, the reader applies backpressure and
    waits for the consumer instead of dropping the oldest frame.

    If the RTSP connection reaches EOF, only this reader exits. main() then
    reconnects the input FFmpeg without stopping the output pipeline.
    """
    log("Frame reader thread started")
    queue_block_start = None

    while not stop_event.is_set() and not reader_stop_event.is_set():
        if decoder.poll() is not None:
            log("Frame reader thread: decoder exited")
            break

        raw = read_exact(decoder.stdout, frame_size)
        if raw is None:
            if not stop_event.is_set() and not reader_stop_event.is_set():
                log("Frame reader thread: RTSP input EOF")
            break

        # No get_nowait()/drop-oldest here.
        # A full queue creates backpressure instead of losing frames.
        while not stop_event.is_set() and not reader_stop_event.is_set():
            try:
                frame_queue.put(raw, timeout=FRAME_QUEUE_PUT_TIMEOUT)
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

    font = cv2.FONT_HERSHEY_SIMPLEX
    scale = max(0.38, round(WIDTH / 750.0, 2))
    text_thickness = 1
    outline_thickness = max(2, int(scale * 3.0))

    line1_y = int(HEIGHT * 0.075)
    line2_y = int(HEIGHT * 0.16)

    # Black outline + white text.
    cv2.putText(
        frame,
        text1,
        (8, line1_y),
        font,
        scale,
        (0, 0, 0),
        outline_thickness,
        cv2.LINE_AA,
    )
    cv2.putText(
        frame,
        text1,
        (8, line1_y),
        font,
        scale,
        (255, 255, 255),
        text_thickness,
        cv2.LINE_AA,
    )

    # Black outline + yellow text.
    cv2.putText(
        frame,
        status,
        (8, line2_y),
        font,
        scale,
        (0, 0, 0),
        outline_thickness,
        cv2.LINE_AA,
    )
    cv2.putText(
        frame,
        status,
        (8, line2_y),
        font,
        scale,
        (0, 255, 255),
        text_thickness,
        cv2.LINE_AA,
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

        caption = (
            "Motion event\n"
            f"Time: {start_text}\n"
            f"Duration: about {duration:.1f}s\n"
            f"Motion regions: {motion_count}\n"
            f"Video: {WIDTH}x{HEIGHT}@{FPS}"
        )

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
    log("ARM64 RTSP motion monitor V2 starting")
    log(f"output: {WIDTH}x{HEIGHT}@{FPS}")
    log(
        f"frame queue: {FRAME_QUEUE_SIZE} frames, "
        f"no-drop backpressure enabled"
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

    rolling_dir = ensure_dir(ROLLING_SEGMENT_DIR)
    event_root = ensure_dir(EVENT_SEGMENT_DIR)
    cleanup_rolling_dir(rolling_dir)

    telegram = TelegramClient(
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
        TELEGRAM_SOCKS5,
    )
    telegram_worker = TelegramWorker(telegram)

    # Input FFmpeg is deliberately restartable. The output FFmpeg stays alive
    # so the UDP stream and rolling segments continue across RTSP reconnects.
    decoder = start_input_ffmpeg()
    encoder = start_output_ffmpeg(rolling_dir)

    def start_decoder_reader(current_decoder):
        reader_stop_event = threading.Event()
        thread = threading.Thread(
            target=frame_reader_worker,
            args=(
                current_decoder,
                frame_size,
                frame_queue,
                STOP_EVENT,
                reader_stop_event,
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

    frame_index = 0
    motion_confirm_count = 0
    frame_size = WIDTH * HEIGHT * 3
    frame_queue = queue.Queue(maxsize=FRAME_QUEUE_SIZE)

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
    total_frames = 0

    try:
        while not STOP_EVENT.is_set():
            if encoder.poll() is not None:
                log("output ffmpeg exited")
                break

            # RTSP input may legitimately disappear temporarily. Reconnect
            # only the input side; do not stop the monitor/output pipeline.
            if decoder.poll() is not None or not reader_thread.is_alive():
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
                raw = frame_queue.get(timeout=1.0)
            except queue.Empty:
                continue

            frame = np.frombuffer(
                raw,
                dtype=np.uint8,
            ).reshape((HEIGHT, WIDTH, 3))

            # One writable C-contiguous copy is required because drawing
            # modifies the frame. Do not make any additional full-frame copies.
            frame = frame.copy()

            now = time.time()

            # ----------------------------------------------------
            # Motion detection every N frames on 160x120.
            # ----------------------------------------------------
            if frame_index % DETECT_EVERY_N_FRAMES == 0:
                gray_small = cv2.cvtColor(
                    frame,
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
            # Brightness. Reuse the 160x120 gray image when it is
            # already available. On the other frames use a small
            # strided sample, avoiding another resize operation.
            # ----------------------------------------------------
            if frame_index % DETECT_EVERY_N_FRAMES == 0:
                brightness = float(
                    np.mean(gray_small)
                )
            else:
                sample = frame[::4, ::4]
                brightness = float(
                    np.mean(
                        0.114 * sample[:, :, 0]
                        + 0.587 * sample[:, :, 1]
                        + 0.299 * sample[:, :, 2]
                    )
                )

            # ----------------------------------------------------
            # Overlay.
            # ----------------------------------------------------
            draw_boxes(
                frame,
                current_boxes,
            )

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

            draw_info(
                frame,
                brightness,
                len(current_boxes),
                event_active,
            )

            # ----------------------------------------------------
            # One raw BGR write. FFmpeg performs the only MJPEG
            # encode and also writes the UDP live stream.
            # ----------------------------------------------------
            try:
                encoder.stdin.write(
                    frame.tobytes()
                )
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
                cv2.imshow(
                    "RTSP Motion",
                    frame,
                )
                key = cv2.waitKey(1) & 0xFF
                if key in (27, ord("q")):
                    STOP_EVENT.set()
                    break

            frame_index += 1
            total_frames += 1
            stat_frames += 1

            elapsed = time.monotonic() - stat_start
            if elapsed >= 5.0:
                log(
                    f"fps={stat_frames / elapsed:.2f} "
                    f"motion={len(current_boxes)} "
                    f"event={event_active}"
                )
                stat_frames = 0
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
