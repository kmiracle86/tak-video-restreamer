"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

ABR (Adaptive Bitrate) HLS Transcoding Service

Manages multi-rendition FFmpeg transcoding processes that produce HLS output
with a master playlist for adaptive bitrate streaming.

Architecture:
- FFmpeg reads from MediaMTX RTSP and writes HLS segments to disk
- When FFmpeg stalls (no new segments), we orphan it and start fresh
- No blocking waits on process termination (handles unkillable D-state processes)
- MediaMTX native HLS is NOT used - we need multi-bitrate ABR which MediaMTX doesn't provide
"""
import os
import json
import subprocess
import threading
import shutil
import logging
import time
from typing import Dict, Optional
import requests

from app.config import MEDIAMTX_API_URL, DATA_DIR

logger = logging.getLogger(__name__)

# Directory where HLS segments and playlists are written
HLS_OUTPUT_DIR = os.environ.get('HLS_OUTPUT_DIR', '/opt/app/hls')

# Directory for FFmpeg stderr logs
FFMPEG_LOG_DIR = os.environ.get('FFMPEG_LOG_DIR', os.path.join(os.environ.get('LOGS_DIR', '/opt/app/logs'), 'ffmpeg'))

# MediaMTX RTSP endpoint for FFmpeg to read from.
# Use 127.0.0.1 (not 'localhost') to avoid IPv6 resolution issues on Windows
# where ::1 may be tried first and hang if MediaMTX only binds IPv4.
MEDIAMTX_RTSP_URL = os.environ.get('MEDIAMTX_RTSP_URL', 'rtsp://127.0.0.1:8554')

# ABR settings file (shared with settings API)
ABR_SETTINGS_FILE = os.path.join(DATA_DIR, 'abr_settings.json')

# Persistent state: which streams have ABR enabled (survives container restarts)
ABR_STATE_FILE = os.path.join(DATA_DIR, 'abr_state.json')

# HLS tuning
HLS_LOW_LATENCY_MODE = os.environ.get('HLS_LOW_LATENCY_MODE', '').lower() == 'true'
HLS_SEGMENT_DURATION = int(os.environ.get('HLS_SEGMENT_DURATION', 2 if HLS_LOW_LATENCY_MODE else 4))
HLS_LIST_SIZE = int(os.environ.get('HLS_LIST_SIZE', 10))

# Stall detection
STALL_CHECK_INTERVAL = 10   # seconds between stall checks
STALL_TIMEOUT = 30          # seconds without playlist update = stall
STARTUP_GRACE = 15          # seconds before stall detection kicks in

# Default ABR rendition presets
DEFAULT_ABR_RENDITIONS = [
    {
        'name': 'high',
        'width': 1280,
        'height': 720,
        'video_bitrate': '2500k',
        'max_rate': '3M',
        'buf_size': '6M',
        'profile': 'main',
        'level': '4.0',
    },
    {
        'name': 'low',
        'width': 640,
        'height': 360,
        'video_bitrate': '600k',
        'max_rate': '800k',
        'buf_size': '1500k',
        'profile': 'baseline',
        'level': '3.0',
    },
]


def _resolution_to_profile(width: int, height: int) -> tuple:
    """Return (profile, level) appropriate for the resolution."""
    pixels = width * height
    if pixels > 921600:   # > 720p
        return ('high', '4.1')
    elif pixels > 409920: # > 540p
        return ('main', '4.0')
    elif pixels > 230400: # > 360p
        return ('main', '3.1')
    else:
        return ('baseline', '3.0')


def _load_renditions_from_settings() -> list:
    """Load ABR renditions from persistent settings file, falling back to defaults."""
    try:
        if os.path.exists(ABR_SETTINGS_FILE):
            with open(ABR_SETTINGS_FILE, 'r') as f:
                settings = json.load(f)
            return _settings_to_renditions(settings)
    except Exception as e:
        logger.warning(f"Could not load ABR settings, using defaults: {e}")
    return list(DEFAULT_ABR_RENDITIONS)


def _settings_to_renditions(settings: dict) -> list:
    """Convert settings dict (from API) to rendition list for FFmpeg."""
    renditions = []
    for tier in ('high', 'medium', 'low'):
        if tier == 'medium' and not settings.get('medium_enabled', False):
            continue
        tier_cfg = settings.get(tier, {})
        res = tier_cfg.get('resolution', '1280x720')
        w, h = (int(x) for x in res.split('x'))
        bv = int(tier_cfg.get('bitrate', 2500))

        profile, level = _resolution_to_profile(w, h)
        max_rate_k = int(bv * 1.2)
        buf_size_k = int(bv * 2.5)

        ba = int(tier_cfg.get('audio_bitrate', 128))

        renditions.append({
            'name': tier,
            'width': w,
            'height': h,
            'video_bitrate': f'{bv}k',
            'max_rate': f'{max_rate_k}k',
            'buf_size': f'{buf_size_k}k',
            'profile': profile,
            'level': level,
            'audio_bitrate': f'{ba}k',
        })
    return renditions if renditions else list(DEFAULT_ABR_RENDITIONS)


class ABRManager:
    """
    Manages ABR transcoding processes for streams.
    
    Design principles:
    - Never block waiting for FFmpeg to die (it may be in unkillable D-state)
    - On stall: orphan old process, start new one immediately
    - State machine per stream: STOPPED -> STARTING -> RUNNING -> (stall) -> RESTARTING
    """

    def __init__(self):
        self._streams: Dict[str, dict] = {}  # stream_name -> state dict
        self._lock = threading.Lock()
        os.makedirs(HLS_OUTPUT_DIR, exist_ok=True)

    # ------------------------------------------------------------------
    # State Persistence
    # ------------------------------------------------------------------

    def _save_state(self):
        """Persist active stream names to disk."""
        try:
            with self._lock:
                active = list(self._streams.keys())
            os.makedirs(os.path.dirname(ABR_STATE_FILE), exist_ok=True)
            with open(ABR_STATE_FILE, 'w') as f:
                json.dump({'streams': active}, f)
        except Exception as e:
            logger.warning(f"Could not save ABR state: {e}")

    def restore_state(self):
        """Restore ABR processes from saved state (called at startup)."""
        if not os.path.exists(ABR_STATE_FILE):
            return
        try:
            with open(ABR_STATE_FILE, 'r') as f:
                state = json.load(f)
            streams = state.get('streams', [])
            if not streams:
                return
            logger.info(f"Restoring ABR state for streams: {streams}")

            def _delayed_restore():
                time.sleep(15)  # Wait for MediaMTX to be ready
                for stream_name in streams:
                    self._wait_for_source_and_start(stream_name)

            t = threading.Thread(target=_delayed_restore, daemon=True, name='abr-restore')
            t.start()
        except Exception as e:
            logger.warning(f"Could not restore ABR state: {e}")

    def _wait_for_source_and_start(self, stream_name: str, max_attempts: int = 10):
        """Wait for RTSP source to be available, then start ABR."""
        for attempt in range(max_attempts):
            try:
                resp = requests.get(
                    f'{MEDIAMTX_API_URL}/v3/paths/get/{stream_name}',
                    timeout=3
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get('ready', False):
                        self.start(stream_name)
                        logger.info(f"ABR auto-restored for {stream_name}")
                        return
            except Exception:
                pass
            time.sleep(5)
        logger.warning(f"ABR restore: {stream_name} never came online, skipping")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self, stream_name: str) -> dict:
        """Start ABR transcoding for a stream."""
        with self._lock:
            if stream_name in self._streams:
                state = self._streams[stream_name]
                if state.get('active'):
                    return {'status': 'already_running', 'stream': stream_name}

            stream_dir = os.path.join(HLS_OUTPUT_DIR, stream_name)
            # Remove any stale playlists/segments from a previous run.
            # Without this, _playlist_response will return 503 "Playlist stale"
            # until FFmpeg rewrites index.m3u8, which can take tens of seconds
            # for high-bitrate sources (e.g. 4K H.265).
            self._cleanup_segments(stream_dir)
            os.makedirs(stream_dir, exist_ok=True)

            state = {
                'active': True,
                'stream_dir': stream_dir,
                'process': None,
                'started_at': 0,
                'restart_count': 0,
                'stop_requested': False,
            }
            self._streams[stream_name] = state

        # Start the monitor thread which handles process lifecycle
        t = threading.Thread(
            target=self._run_loop,
            args=(stream_name,),
            daemon=True,
            name=f"abr-{stream_name}",
        )
        t.start()

        self._save_state()
        return {'status': 'started', 'stream': stream_name}

    def stop(self, stream_name: str) -> dict:
        """Stop ABR transcoding for a stream."""
        with self._lock:
            state = self._streams.pop(stream_name, None)

        if state is None:
            return {'status': 'not_running', 'stream': stream_name}

        state['stop_requested'] = True
        state['active'] = False

        # Fire SIGKILL at current process, don't wait
        proc = state.get('process')
        if proc and proc.poll() is None:
            try:
                proc.kill()  # SIGKILL - fire and forget
            except (ProcessLookupError, PermissionError, OSError):
                pass

        # Clean up HLS segments
        self._cleanup_segments(state['stream_dir'])
        logger.info(f"ABR stopped: {stream_name}")
        self._save_state()
        return {'status': 'stopped', 'stream': stream_name}

    def status(self, stream_name: str) -> dict:
        """Return status of ABR transcoding for a stream."""
        with self._lock:
            state = self._streams.get(stream_name)

        if state is None:
            return {'running': False, 'stream': stream_name}

        proc = state.get('process')
        running = proc is not None and proc.poll() is None

        return {
            'running': state.get('active', False),
            'stream': stream_name,
            'pid': proc.pid if proc else None,
            'restart_count': state.get('restart_count', 0),
            'uptime_seconds': int(time.time() - state['started_at']) if running else 0,
        }

    def list_active(self) -> list:
        """List all streams with active ABR transcoding."""
        now = time.time()
        with self._lock:
            result = []
            for name, state in self._streams.items():
                proc = state.get('process')
                running = proc is not None and proc.poll() is None
                result.append({
                    'stream': name,
                    'running': state.get('active', False),
                    'restart_count': state.get('restart_count', 0),
                    'uptime_seconds': int(now - state['started_at']) if running else 0,
                })
            return result

    def stop_all(self):
        """Stop all ABR processes (for shutdown)."""
        with self._lock:
            names = list(self._streams.keys())
        for name in names:
            self.stop(name)

    def get_hls_dir(self, stream_name: str) -> Optional[str]:
        """Return the HLS output directory for a stream, or None."""
        d = os.path.join(HLS_OUTPUT_DIR, stream_name)
        return d if os.path.isdir(d) else None

    def apply_settings(self, settings: dict):
        """Apply new ABR settings by restarting all active ABR processes."""
        with self._lock:
            active_streams = list(self._streams.keys())
        for stream_name in active_streams:
            logger.info(f"Restarting ABR for {stream_name} with new settings")
            self.stop(stream_name)
            time.sleep(1)
            self.start(stream_name)

    # ------------------------------------------------------------------
    # Process Lifecycle Loop
    # ------------------------------------------------------------------

    def _run_loop(self, stream_name: str):
        """
        Main loop for a stream's ABR process.
        
        Handles:
        - Starting FFmpeg
        - Detecting stalls via playlist mtime
        - Restarting on stall or crash (orphan old process, start new one)
        - Exiting when stop is requested
        """
        while True:
            with self._lock:
                state = self._streams.get(stream_name)
                if state is None or state.get('stop_requested'):
                    return

            # Start FFmpeg
            proc = self._start_ffmpeg(stream_name, state['stream_dir'])
            if proc is None:
                logger.error(f"ABR {stream_name}: failed to start FFmpeg, retrying in 10s")
                time.sleep(10)
                continue

            with self._lock:
                state = self._streams.get(stream_name)
                if state is None:
                    self._fire_and_forget_kill(proc)
                    return
                state['process'] = proc
                state['started_at'] = time.time()

            logger.info(f"ABR {stream_name}: FFmpeg started (pid={proc.pid})")

            # Monitor loop
            exit_reason = self._monitor_process(stream_name, proc, state)

            if exit_reason == 'stopped':
                self._close_ffmpeg_log(proc)
                return
            elif exit_reason == 'stall':
                self._log_ffmpeg_stderr(stream_name, proc, 'stall')
                with self._lock:
                    st = self._streams.get(stream_name)
                    if st:
                        st['restart_count'] = st.get('restart_count', 0) + 1
                logger.warning(f"ABR {stream_name}: stall detected, restarting FFmpeg")
                self._fire_and_forget_kill(proc)
                self._close_ffmpeg_log(proc)
                time.sleep(2)
            elif exit_reason == 'exited':
                self._log_ffmpeg_stderr(stream_name, proc, 'exit')
                with self._lock:
                    st = self._streams.get(stream_name)
                    if st:
                        st['restart_count'] = st.get('restart_count', 0) + 1
                logger.info(f"ABR {stream_name}: FFmpeg exited, restarting in 5s")
                self._close_ffmpeg_log(proc)
                time.sleep(5)

    def _monitor_process(self, stream_name: str, proc, state: dict) -> str:
        """
        Monitor FFmpeg process for exit or stall.
        
        Returns:
        - 'stopped' if stop was requested
        - 'exited' if process exited on its own
        - 'stall' if no new segments for STALL_TIMEOUT seconds
        """
        started_at = time.time()

        while True:
            time.sleep(STALL_CHECK_INTERVAL)

            # Check if stop requested
            with self._lock:
                st = self._streams.get(stream_name)
                if st is None or st.get('stop_requested'):
                    return 'stopped'

            # Check if process exited
            if proc.poll() is not None:
                return 'exited'

            # Check for stall (after startup grace period)
            elapsed = time.time() - started_at
            if elapsed > STARTUP_GRACE:
                try:
                    from app.api.settings import server_settings
                    stall_enabled = server_settings.get('stall_detection_enabled', True)
                except Exception:
                    stall_enabled = True
                if stall_enabled and self._is_stalled(state['stream_dir'], started_at):
                    return 'stall'

    def _is_stalled(self, stream_dir: str, started_at: float) -> bool:
        """
        Check if stream is stalled by looking at variant playlist mtimes.
        
        We check index.m3u8 (not segment files) because FFmpeg may be
        partially writing a .ts file while the stream is actually frozen.
        Playlist only updates when a segment COMPLETES.

        Playlists older than started_at are from a previous run and are ignored
        (prevents false stall detection on startup when stale files exist on disk).
        """
        newest_mtime = 0
        try:
            for variant in ('v0', 'v1', 'v2'):
                playlist = os.path.join(stream_dir, variant, 'index.m3u8')
                if os.path.isfile(playlist):
                    mt = os.path.getmtime(playlist)
                    # Ignore files that predate this FFmpeg process
                    if mt > started_at and mt > newest_mtime:
                        newest_mtime = mt
        except OSError:
            return False

        if newest_mtime == 0:
            return False  # No playlist written by this process yet - still starting

        age = time.time() - newest_mtime
        try:
            from app.api.settings import server_settings
            stall_timeout = server_settings.get('stall_threshold_seconds', STALL_TIMEOUT)
        except Exception:
            stall_timeout = STALL_TIMEOUT
        if age > stall_timeout:
            logger.info(f"Stall check: playlist age={age:.0f}s > timeout={stall_timeout}s in {stream_dir}")
            return True
        return False

    # ------------------------------------------------------------------
    # FFmpeg Management
    # ------------------------------------------------------------------

    def _start_ffmpeg(self, stream_name: str, stream_dir: str) -> Optional[subprocess.Popen]:
        """Build and start the FFmpeg ABR command."""
        cmd = self._build_ffmpeg_cmd(stream_name, stream_dir)
        if cmd is None:
            return None

        logger.info(f"ABR {stream_name}: cmd={' '.join(cmd)}")

        # Redirect stderr to a log file instead of PIPE to prevent deadlock.
        # subprocess.PIPE has a ~64KB OS buffer; if FFmpeg fills it and nobody
        # reads, FFmpeg blocks on write() and the stream freezes.
        log_path = os.path.join(FFMPEG_LOG_DIR, f'{stream_name}.log')
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        try:
            stderr_file = open(log_path, 'w')
        except OSError as e:
            logger.error(f"ABR {stream_name}: cannot open log file {log_path}: {e}")
            stderr_file = subprocess.DEVNULL

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=stderr_file,
                stdin=subprocess.DEVNULL,
                start_new_session=True,  # Own process group for clean orphaning
            )
            # Store log path and file handle for later diagnostics
            proc._stderr_log_path = log_path
            proc._stderr_file = stderr_file
            return proc
        except Exception as e:
            logger.error(f"ABR {stream_name}: failed to start FFmpeg: {e}")
            if hasattr(stderr_file, 'close'):
                stderr_file.close()
            return None

    def _build_ffmpeg_cmd(self, stream_name: str, stream_dir: str) -> Optional[list]:
        """Build the FFmpeg command for ABR HLS transcoding."""
        source_url = f'{MEDIAMTX_RTSP_URL}/{stream_name}'
        renditions = _load_renditions_from_settings()

        has_audio = self._source_has_audio(source_url)
        logger.info(f"ABR build cmd: stream={stream_name}, renditions={len(renditions)}, has_audio={has_audio}")

        cmd = [
            'ffmpeg',
            '-loglevel', 'warning',   # Suppress frame= progress lines (prevents log bloat)
            '-rtsp_transport', self._get_rtsp_transport(),
            # Tolerate H.265 reference-frame errors and regenerate PTS so the
            # filter chain never sees huge timestamp gaps (which cause ~1000 frame
            # duplications and stall the first HLS segment from being written).
            '-err_detect', 'ignore_err',
            '-fflags', '+genpts+discardcorrupt+nobuffer',
            '-flags', 'low_delay',
            '-analyzeduration', '2000000',
            '-probesize', '2000000',
            '-i', source_url,
        ]

        # Build filter complex for scaling
        # format=yuv420p is explicit here so that high-bit-depth inputs (e.g. 10-bit AV1,
        # 10-bit HEVC) are correctly converted before reaching the libx264 encoder,
        # which requires 8-bit yuv420p.
        filter_parts = []
        for idx, r in enumerate(renditions):
            w, h = r['width'], r['height']
            filter_parts.append(
                f"[0:v]scale={w}:{h}:force_original_aspect_ratio=decrease,"
                f"pad={w}:{h}:(ow-iw)/2:(oh-ih)/2,"
                f"format=yuv420p[v{idx}]"
            )
        cmd += ['-filter_complex', ';'.join(filter_parts)]

        # Output mappings per rendition
        for idx, r in enumerate(renditions):
            cmd += [
                '-map', f'[v{idx}]',
                f'-c:v:{idx}', 'libx264',
                '-preset', 'ultrafast',
                '-tune', 'zerolatency',
                f'-profile:v:{idx}', r['profile'],
                f'-level:v:{idx}', r['level'],
                '-pix_fmt', 'yuv420p',
                f'-b:v:{idx}', r['video_bitrate'],
                f'-maxrate:v:{idx}', r['max_rate'],
                f'-bufsize:v:{idx}', r['buf_size'],
                '-g', str(HLS_SEGMENT_DURATION * 30),  # GOP = segment duration * fps
                '-keyint_min', str(HLS_SEGMENT_DURATION * 30),
                '-sc_threshold', '0',
            ]

        # Audio (if present)
        # Use audio bitrate from the highest-tier rendition (applied to the
        # single shared audio rendition in the HLS EXT-X-MEDIA group).
        if has_audio:
            audio_br = renditions[0].get('audio_bitrate', '128k')
            cmd += ['-map', '0:a', '-c:a', 'aac', '-b:a', audio_br, '-ar', '48000', '-ac', '2']

        # HLS output var_stream_map.
        # IMPORTANT: do NOT use `v:0,a:0 v:1,a:0` — mapping the same audio output
        # stream into two variants triggers "Same elementary stream found more than
        # once in two different variant definitions" and FFmpeg exits immediately.
        # Use an EXT-X-MEDIA audio group instead: all video variants share one audio
        # rendition (outputting to v{N}/) via agroup.
        if has_audio:
            video_parts = ' '.join([f'v:{i},agroup:audio' for i in range(len(renditions))])
            var_stream_map = f'{video_parts} a:0,agroup:audio,default:yes'
        else:
            var_stream_map = ' '.join([f'v:{i}' for i in range(len(renditions))])

        cmd += [
            '-f', 'hls',
            '-hls_time', str(HLS_SEGMENT_DURATION),
            '-hls_list_size', str(HLS_LIST_SIZE),
            '-hls_delete_threshold', '20',
            '-hls_flags', 'delete_segments+independent_segments+append_list',
            '-hls_segment_type', 'mpegts',
            '-master_pl_name', 'master.m3u8',
            '-var_stream_map', var_stream_map,
            '-hls_segment_filename', os.path.join(stream_dir, 'v%v', 'segment%d.ts'),
            os.path.join(stream_dir, 'v%v', 'index.m3u8'),
        ]

        # Ensure output directories exist (video variants + audio rendition if present)
        n_output_streams = len(renditions) + (1 if has_audio else 0)
        for idx in range(n_output_streams):
            os.makedirs(os.path.join(stream_dir, f'v{idx}'), exist_ok=True)

        return cmd

    @staticmethod
    def _get_rtsp_transport() -> str:
        """Get rtsp_transport from server_settings."""
        try:
            from app.api.settings import server_settings
            return server_settings.get('rtsp_transport', 'tcp')
        except Exception:
            return 'tcp'

    @staticmethod
    def _source_has_audio(source_url: str) -> bool:
        """Probe the RTSP source to check for audio tracks."""
        try:
            transport = ABRManager._get_rtsp_transport()
            result = subprocess.run(
                [
                    'ffprobe', '-v', 'quiet', '-rtsp_transport', transport,
                    '-show_streams', '-select_streams', 'a',
                    '-of', 'json', source_url
                ],
                capture_output=True,
                timeout=10,
            )
            if result.returncode == 0:
                data = json.loads(result.stdout)
                return len(data.get('streams', [])) > 0
        except Exception:
            pass
        return False

    @staticmethod
    def _close_ffmpeg_log(proc: subprocess.Popen):
        """Close the stderr log file handle if present."""
        f = getattr(proc, '_stderr_file', None)
        if f and hasattr(f, 'close'):
            try:
                f.close()
            except Exception:
                pass

    @staticmethod
    def _log_ffmpeg_stderr(stream_name: str, proc: subprocess.Popen, reason: str):
        """Log the last lines of FFmpeg's stderr log file for diagnostics."""
        log_path = getattr(proc, '_stderr_log_path', None)
        if not log_path or not os.path.isfile(log_path):
            return
        try:
            with open(log_path, 'r') as f:
                lines = f.readlines()
            # Log last 15 lines (skip empty)
            tail = [l.rstrip() for l in lines[-15:] if l.strip()]
            if tail:
                logger.info(f"ABR {stream_name}: FFmpeg stderr ({reason}, pid={proc.pid}):")
                for line in tail:
                    logger.info(f"  ffmpeg[{stream_name}]: {line}")
        except Exception as e:
            logger.debug(f"Could not read FFmpeg log for {stream_name}: {e}")

    @staticmethod
    def _fire_and_forget_kill(proc: subprocess.Popen):
        """Send SIGKILL to process without waiting. Process may be in D-state."""
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.kill()  # SIGKILL - does not block
        except (ProcessLookupError, PermissionError, OSError):
            pass
        # Do NOT call proc.wait() - that would block on D-state processes

    @staticmethod
    def _cleanup_segments(stream_dir: str):
        """Remove HLS segments directory for a stream."""
        try:
            if os.path.isdir(stream_dir):
                shutil.rmtree(stream_dir)
                logger.info(f"Cleaned up HLS segments: {stream_dir}")
        except Exception as e:
            logger.error(f"Error cleaning HLS segments {stream_dir}: {e}")


# Singleton instance
abr_manager = ABRManager()
