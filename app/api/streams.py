"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

Streams API Blueprint - Stream management endpoints
"""
from flask import Blueprint, request, jsonify
from datetime import datetime, timezone
import subprocess
import traceback
import threading
import time
import os
from pathlib import Path

from app.config import (
    MEDIAMTX_API_URL, DATA_DIR,
    PULL_STREAM_BUFFER_SIZE, PULL_STREAM_MAX_DELAY
)
from app.state import (
    active_recordings, active_pull_streams, pull_stream_configs,
    thumbnail_executor, post_processing_queue,
    recording_lock, pull_stream_lock, hidden_streams, hidden_streams_lock
)
from app.services.mediamtx import MediaMTXClient
from app.utils.codec_detection import detect_stream_codec, analyze_recording
from app.utils.thumbnail import generate_thumbnail
from app.websocket.broadcast import broadcast
import logging
import json
import requests as http_requests

logger = logging.getLogger(__name__)

streams_bp = Blueprint('streams', __name__)
mediamtx = MediaMTXClient(MEDIAMTX_API_URL)

# Track last time bytes were received per stream (for last_data_time)
_stream_bytes_tracker: dict = {}  # {stream_name: {'bytes': int, 'last_change': float}}

# Streams explicitly stopped/deleted via an endpoint — the loop should exit silently
# (the endpoint already broadcast the appropriate event).
_externally_stopped: set = set()

# ---------------------------------------------------------------------------
# Pull-stream persistence (survives container restarts)
# ---------------------------------------------------------------------------
_PULL_SOURCES_FILE = os.path.join(DATA_DIR, 'pull_sources.json')
# Format: {stream_name: {source_url, username, password}}
_pull_sources: dict = {}

# ---------------------------------------------------------------------------
# IP blocklist persistence (survives container restarts)
# ---------------------------------------------------------------------------
_BLOCKED_IPS_FILE = os.path.join(DATA_DIR, 'blocked_ips.json')
_blocked_ips: set = set()


def _load_pull_sources():
    """Load persisted pull stream configs from disk."""
    global _pull_sources
    try:
        if os.path.isfile(_PULL_SOURCES_FILE):
            with open(_PULL_SOURCES_FILE, 'r') as f:
                raw = json.load(f)
            # Migrate old format {name: url_string} -> {name: {source_url: ...}}
            migrated = {}
            for k, v in raw.items():
                if isinstance(v, str):
                    migrated[k] = {'source_url': v, 'username': '', 'password': ''}
                else:
                    migrated[k] = v
            _pull_sources = migrated
    except Exception as e:
        logger.warning(f"Could not load pull_sources.json: {e}")
        _pull_sources = {}


def _save_pull_source(stream_name: str, source_url: str, username: str = '', password: str = ''):
    """Persist a pull stream's config to disk (atomic write)."""
    _pull_sources[stream_name] = {'source_url': source_url, 'username': username, 'password': password}
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = _PULL_SOURCES_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(_pull_sources, f, indent=2)
        os.replace(tmp, _PULL_SOURCES_FILE)
    except Exception as e:
        logger.warning(f"Could not save pull_sources.json: {e}")


def _remove_pull_source(stream_name: str):
    """Remove a pull stream's persisted config so it won't be restored on restart."""
    if stream_name in _pull_sources:
        del _pull_sources[stream_name]
        try:
            tmp = _PULL_SOURCES_FILE + '.tmp'
            with open(tmp, 'w') as f:
                json.dump(_pull_sources, f, indent=2)
            os.replace(tmp, _PULL_SOURCES_FILE)
        except Exception as e:
            logger.warning(f"Could not update pull_sources.json: {e}")


# ---------------------------------------------------------------------------
# Blocklist helpers
# ---------------------------------------------------------------------------

def _load_blocked_ips():
    """Load persisted IP blocklist from disk."""
    global _blocked_ips
    try:
        if os.path.isfile(_BLOCKED_IPS_FILE):
            with open(_BLOCKED_IPS_FILE, 'r') as f:
                data = json.load(f)
            _blocked_ips = set(data) if isinstance(data, list) else set()
    except Exception as e:
        logger.warning(f"Could not load blocked_ips.json: {e}")
        _blocked_ips = set()


def _save_blocked_ips():
    """Persist IP blocklist to disk (atomic write)."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = _BLOCKED_IPS_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(sorted(_blocked_ips), f, indent=2)
        os.replace(tmp, _BLOCKED_IPS_FILE)
    except Exception as e:
        logger.warning(f"Could not save blocked_ips.json: {e}")


def _block_ip(ip: str):
    """Add an IP to the blocklist and persist."""
    _blocked_ips.add(ip)
    _save_blocked_ips()
    logger.info(f"Blocked IP: {ip}")


def _unblock_ip(ip: str):
    """Remove an IP from the blocklist and persist."""
    _blocked_ips.discard(ip)
    _save_blocked_ips()
    logger.info(f"Unblocked IP: {ip}")


def get_blocked_ips() -> set:
    """Public accessor for the blocklist (used by the enforcement loop)."""
    return _blocked_ips


def _start_pull_impl(stream_name: str, source_url: str, username: str = '', password: str = ''):
    """Core logic to launch a pull stream FFmpeg process and its monitor thread.
    Called both from the API endpoint and from the startup restore path.
    Raises on error.
    """
    ffmpeg_args = _build_pull_ffmpeg_args(source_url, stream_name)
    logger.info(f"Starting pull stream: {stream_name} from {source_url}")

    process = subprocess.Popen(
        ffmpeg_args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    with pull_stream_lock:
        active_pull_streams[stream_name] = process
        pull_stream_configs[stream_name] = {
            'source_url': source_url,
            'username': username,
            'password': password,
            'retry_count': 0,
            'auto_retry': True,
            'start_time': time.time()
        }

    _save_pull_source(stream_name, source_url, username, password)
    threading.Thread(target=_pull_stream_loop, args=(stream_name,), daemon=True).start()
    return process


_load_pull_sources()
_load_blocked_ips()

# Guard flag to prevent duplicate restore on re-import
_restore_started = False


def _restore_pull_streams():
    """Re-launch persisted pull streams on startup (runs in a background thread)."""
    global _restore_started
    if _restore_started:
        return
    _restore_started = True
    
    if not _pull_sources:
        return

    def _do_restore():
        time.sleep(10)  # Give MediaMTX time to finish starting up
        for stream_name, cfg in list(_pull_sources.items()):
            # Skip if already running (e.g. started by something else)
            if stream_name in active_pull_streams:
                continue
            source_url = cfg.get('source_url', '')
            if not source_url:
                continue
            try:
                _start_pull_impl(
                    stream_name,
                    source_url,
                    cfg.get('username', ''),
                    cfg.get('password', '')
                )
                broadcast('pull_stream_started', {'name': stream_name, 'source': source_url})
                logger.info(f"Pull stream auto-restored: {stream_name} <- {source_url}")
            except Exception as e:
                logger.error(f"Failed to restore pull stream {stream_name}: {e}")

    threading.Thread(target=_do_restore, daemon=True, name='pull-restore').start()


_restore_pull_streams()

# Map MediaMTX source types to human-readable protocol labels
_SOURCE_TYPE_LABELS = {
    'srtConn': 'SRT',
    'rtspSession': 'RTSP',
    'rtspSource': 'RTSP Pull',
    'rtmpConn': 'RTMP',
    'webRTCSession': 'WebRTC',
    'hlsSource': 'HLS',
    'rpiCameraSource': 'RPi Camera',
}

# Map MediaMTX source types to their connection-list API endpoint
_SOURCE_TYPE_ENDPOINTS = {
    'srtConn': '/v3/srtconns/list',
    'rtspSession': '/v3/rtspsessions/list',
    'rtmpConn': '/v3/rtmpconns/list',
    'webRTCSession': '/v3/webrtcsessions/list',
}


def _fetch_connection_map() -> dict:
    """Fetch all active connections from MediaMTX once per list_streams() call.

    Returns {connection_id: remote_address_without_port} for all tracked protocols.
    Calling this once per request instead of once per stream eliminates N redundant
    HTTP calls when listing N streams that share the same protocol endpoint.
    """
    result = {}
    for endpoint in _SOURCE_TYPE_ENDPOINTS.values():
        try:
            resp = http_requests.get(f'{MEDIAMTX_API_URL}{endpoint}', timeout=3)
            if resp.status_code == 200:
                for item in resp.json().get('items', []):
                    conn_id = item.get('id', '')
                    raw = item.get('remoteAddr', '')
                    if conn_id and raw:
                        result[conn_id] = raw.rsplit(':', 1)[0]
        except Exception as e:
            logger.debug(f"Could not fetch connections from {endpoint}: {e}")
    return result


def _resolve_source_info(source: dict | None, conn_map: dict | None = None) -> dict:
    """Look up protocol label and remote address for a stream source.

    Returns dict with 'protocol' (str) and 'sourceAddress' (str or None).
    Pass conn_map (from _fetch_connection_map) to avoid a per-stream HTTP call.
    """
    if not source or not isinstance(source, dict):
        return {'protocol': 'Unknown', 'sourceAddress': None}

    src_type = source.get('type', '')
    src_id = source.get('id', '')
    protocol = _SOURCE_TYPE_LABELS.get(src_type, src_type or 'Unknown')
    address = None

    if conn_map is not None:
        # Use pre-fetched map — zero extra HTTP calls
        address = conn_map.get(src_id) if src_id else None
    else:
        # Fallback: individual fetch (used by single-stream GET endpoints)
        endpoint = _SOURCE_TYPE_ENDPOINTS.get(src_type)
        if endpoint and src_id:
            try:
                resp = http_requests.get(f'{MEDIAMTX_API_URL}{endpoint}', timeout=3)
                if resp.status_code == 200:
                    for item in resp.json().get('items', []):
                        if item.get('id') == src_id:
                            raw = item.get('remoteAddr', '')
                            if raw:
                                address = raw.rsplit(':', 1)[0]
                            break
            except Exception as e:
                logger.debug(f"Could not resolve source address for {src_type}/{src_id}: {e}")

    return {'protocol': protocol, 'sourceAddress': address}


HLS_MUXABLE_CODECS = {'AV1', 'VP9', 'H265', 'H264', 'Opus', 'MPEG-4 Audio'}


def _needs_transcode(tracks: list) -> bool:
    if not tracks:
        return False
    return not any(track in HLS_MUXABLE_CODECS for track in tracks)


def _extract_stream_info(path_name: str, path_info: dict, conn_map: dict | None = None) -> dict:
    """Extract stream info from a MediaMTX path entry (deduplicates list/dict handling)."""
    readers = path_info.get('readers', [])
    num_readers = len(readers) if readers else 0

    bytes_received = 0
    bytes_sent = 0

    # bytesReceived: direct field or from source object
    if 'bytesReceived' in path_info:
        bytes_received = path_info.get('bytesReceived', 0)
    elif 'source' in path_info:
        source = path_info.get('source')
        if source and isinstance(source, dict):
            bytes_received = source.get('bytesReceived', 0)

    # bytesSent: direct field or sum from readers
    if 'bytesSent' in path_info:
        bytes_sent = path_info.get('bytesSent', 0)
    elif readers and isinstance(readers, list):
        for reader in readers:
            if isinstance(reader, dict):
                bytes_sent += reader.get('bytesSent', 0)

    # Resolve source protocol and origin IP
    source_info = _resolve_source_info(path_info.get('source'), conn_map)

    # Full source URL — check in-memory config first, then persisted file
    source_url = None
    with pull_stream_lock:
        config = pull_stream_configs.get(path_name)
        if config:
            source_url = config.get('source_url')
    if not source_url:
        persisted = _pull_sources.get(path_name)
        if persisted:
            source_url = persisted.get('source_url') if isinstance(persisted, dict) else persisted

    # Track last time bytes_received changed (data flowing indicator)
    now = time.time()
    prev = _stream_bytes_tracker.get(path_name)
    if prev is None or prev['bytes'] != bytes_received:
        _stream_bytes_tracker[path_name] = {'bytes': bytes_received, 'last_change': now}
        last_data_time = now
    else:
        last_data_time = prev['last_change']

    tracks = path_info.get('tracks') or []

    return {
        'name': path_name,
        'ready': path_info.get('ready', False),
        'numReaders': num_readers,
        'bytesReceived': bytes_received,
        'bytesSent': bytes_sent,
        'recording': path_name in active_recordings,
        'protocol': source_info['protocol'],
        'sourceAddress': source_info['sourceAddress'],
        'sourceUrl': source_url,
        'tracks': tracks,
        'needsTranscode': _needs_transcode(tracks),
        'lastDataTime': datetime.fromtimestamp(last_data_time, tz=timezone.utc).isoformat() if bytes_received > 0 else None,
    }


def _terminate_process(process, timeout: int = 5):
    """Terminate a subprocess gracefully, falling back to kill."""
    process.terminate()
    try:
        process.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def _stop_stream_components(stream_name: str, remove_pull_config: bool = False) -> tuple:
    """
    Stop all components for a stream (recording, pull, tests, connections).
    Returns (stopped_components, kicked_count).
    """
    stopped_components = []

    # Stop recording if active
    with recording_lock:
        if stream_name in active_recordings:
            recording = active_recordings[stream_name]
            _terminate_process(recording['process'])
            del active_recordings[stream_name]
            logger.info(f"Stopped recording for stream: {stream_name}")
            stopped_components.append('recording')

    # Stop pull stream if active
    with pull_stream_lock:
        if stream_name in active_pull_streams:
            try:
                _terminate_process(active_pull_streams[stream_name])
                if stream_name in active_pull_streams:
                    del active_pull_streams[stream_name]
                logger.info(f"Stopped pull stream: {stream_name}")
                stopped_components.append('pull stream')
            except KeyError:
                logger.info(f"Pull stream {stream_name} already stopped by monitor thread")

        # Handle pull config
        if stream_name in pull_stream_configs:
            if remove_pull_config:
                del pull_stream_configs[stream_name]
                _remove_pull_source(stream_name)
            else:
                pull_stream_configs[stream_name]['auto_retry'] = False
        # Mark as externally stopped so the monitor loop exits without re-broadcasting
        _externally_stopped.add(stream_name)

    # Stop test pattern publishers if active
    from app.api.test import active_tests
    tests_to_stop = [
        tid for tid, info in list(active_tests.items())
        if info.get('stream_name') == stream_name
    ]
    for test_id in tests_to_stop:
        test_info = active_tests[test_id]
        process = test_info['process']
        if process.poll() is None:
            _terminate_process(process)
        del active_tests[test_id]
        logger.info(f"Stopped test publisher {test_id} for stream: {stream_name}")
        stopped_components.append('test publisher')

    # Kick all connections for this stream (across all protocol types)
    connections = mediamtx.list_connections()
    kicked_count = 0
    if connections:
        for conn in connections:
            conn_path = conn.get('path', '')
            conn_id = conn.get('id', '')
            conn_type = conn.get('_conn_type', '')
            if conn_path == stream_name and conn_id and conn_type:
                if mediamtx.kick_connection(conn_type, conn_id):
                    kicked_count += 1
                    logger.info(f"Kicked {conn_type} connection {conn_id} for stream {stream_name}")

    # Remove the path from MediaMTX so it no longer appears in the stream list
    if remove_pull_config:
        if mediamtx.delete_path(stream_name):
            logger.info(f"Deleted MediaMTX path config for: {stream_name}")
            stopped_components.append('mediamtx path')
        else:
            logger.info(f"No explicit config for {stream_name} (regex-matched)")
        # Hide phantom paths that linger because of regex catch-all configs
        with hidden_streams_lock:
            hidden_streams.add(stream_name)
        logger.info(f"Marked {stream_name} as hidden")

    return stopped_components, kicked_count


def _build_pull_ffmpeg_args(source_url: str, stream_name: str) -> list:
    """Build FFmpeg args for pull stream re-publishing."""
    from app.api.settings import server_settings
    transport = server_settings.get('rtsp_transport', 'tcp')
    timeout_us = server_settings.get('connection_timeout', 5000000)
    is_rtsp = source_url.lower().startswith(('rtsp://', 'rtsps://'))
    args = ['ffmpeg']
    if is_rtsp:
        args.extend([
            '-rtsp_transport', transport,
            '-timeout', str(timeout_us),
        ])
    args.extend([
        '-buffer_size', str(PULL_STREAM_BUFFER_SIZE),
        '-max_delay', str(PULL_STREAM_MAX_DELAY),
        '-analyzeduration', '2000000',
        '-probesize', '2000000',
    ])
    # -reconnect options are only supported by the HTTP protocol handler;
    # applying them to RTSP causes "Option reconnect not found" in FFmpeg 7+.
    if not is_rtsp and server_settings.get('enable_ffmpeg_reconnect', True):
        args.extend([
            '-reconnect', '1',
            '-reconnect_streamed', '1',
            '-reconnect_delay_max', '5',
        ])
    args.extend([
        '-i', source_url,
        '-map', '0',
        '-err_detect', 'ignore_err',
        '-fflags', '+genpts+discardcorrupt+nobuffer',
        '-flags', 'low_delay',
        '-c', 'copy',
        '-f', 'rtsp',
        '-rtsp_transport', transport,
        f'rtsp://localhost:8554/{stream_name}'
    ])
    return args


def _finalize_recording_for_reconnect(stream_name: str):
    """Gracefully stop an active recording before a pull stream reconnect."""
    if stream_name not in active_recordings:
        return
    logger.info(f"Finalizing recording for {stream_name} before reconnection")
    try:
        recording_info = active_recordings[stream_name]
        recording_process = recording_info.get('process')
        if recording_process and recording_process.poll() is None:
            recording_process.stdin.write(b'q')
            recording_process.stdin.flush()
            try:
                recording_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                recording_process.terminate()
                recording_process.wait(timeout=5)
    except Exception as e:
        logger.error(f"Error finalizing recording during retry: {e}")


def _pull_stream_loop(stream_name: str):
    """
    Iterative pull stream monitor with auto-retry.
    Runs in a background thread — loops until retry is disabled or exhausted.
    Reads reconnect settings from server_settings at runtime.
    """
    from app.api.settings import server_settings

    while True:
        config = pull_stream_configs.get(stream_name)
        if not config:
            break

        # Wait for the current process to finish
        with pull_stream_lock:
            process = active_pull_streams.get(stream_name)
        if not process:
            break

        # Drain stderr and wait for exit
        stderr_output = []
        for line in process.stderr:
            stderr_output.append(line.decode('utf-8', errors='ignore'))
        return_code = process.wait()

        if return_code != 0:
            logger.error(f"Pull stream FFmpeg exited with code {return_code} for {stream_name}")
            logger.error(f"FFmpeg stderr: {''.join(stderr_output[-20:])}")

        # Clean up process reference
        with pull_stream_lock:
            if stream_name in active_pull_streams:
                del active_pull_streams[stream_name]

        # Read current reconnect settings
        auto_reconnect = server_settings.get('auto_reconnect', True)
        max_attempts = server_settings.get('max_reconnect_attempts', -1)
        base_delay = server_settings.get('reconnect_delay', 5)
        use_backoff = server_settings.get('exponential_backoff', False)
        max_backoff = server_settings.get('max_backoff_delay', 60)

        # Check if we should auto-retry
        config = pull_stream_configs.get(stream_name)
        if not config or not config.get('auto_retry', True) or not auto_reconnect:
            # No auto-retry — clean up and exit
            with pull_stream_lock:
                if stream_name in pull_stream_configs:
                    del pull_stream_configs[stream_name]
            # Only broadcast if this wasn't triggered by an explicit stop/delete endpoint
            # (those already broadcast stream_stopped / stream_deleted)
            if stream_name not in _externally_stopped:
                broadcast('pull_stream_stopped', {'name': stream_name, 'code': return_code})
            else:
                _externally_stopped.discard(stream_name)
            break

        if max_attempts != -1 and config['retry_count'] >= max_attempts:
            logger.error(f"Pull stream {stream_name} exceeded max retry attempts ({max_attempts})")
            with pull_stream_lock:
                if stream_name in pull_stream_configs:
                    del pull_stream_configs[stream_name]
            broadcast('pull_stream_failed', {'name': stream_name, 'reason': 'max_retries_exceeded'})
            break

        config['retry_count'] += 1

        # Calculate delay with optional exponential backoff
        if use_backoff:
            delay = min(base_delay * (2 ** (config['retry_count'] - 1)), max_backoff)
        else:
            delay = base_delay

        logger.info(f"Pull stream disconnected for {stream_name}, retrying in {delay}s (attempt {config['retry_count']})")

        # Finalize any active recording before reconnect
        _finalize_recording_for_reconnect(stream_name)

        broadcast('pull_stream_retrying', {
            'name': stream_name,
            'code': return_code,
            'retry_count': config['retry_count'],
            'retry_delay': delay
        })

        time.sleep(delay)

        # Check again after sleep — config may have been removed
        if stream_name not in pull_stream_configs:
            logger.info(f"Pull stream {stream_name} config removed during retry delay, stopping")
            break
        if stream_name in active_pull_streams:
            logger.info(f"Pull stream {stream_name} already reconnected, stopping retry loop")
            break

        # Start a new FFmpeg process
        try:
            source_url = config['source_url']
            ffmpeg_args = _build_pull_ffmpeg_args(source_url, stream_name)
            new_process = subprocess.Popen(
                ffmpeg_args,
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            with pull_stream_lock:
                active_pull_streams[stream_name] = new_process
            broadcast('pull_stream_reconnected', {
                'name': stream_name,
                'source': source_url,
                'retry_count': config['retry_count']
            })
        except Exception as e:
            logger.error(f"Error starting pull stream retry for {stream_name}: {e}")
            time.sleep(delay)
            continue  # Try again next iteration


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

@streams_bp.route('/api/streams', methods=['GET'])
def list_streams():
    """List all active streams from MediaMTX"""
    try:
        paths_data = mediamtx.list_paths()
        if not paths_data:
            return jsonify([])

        conn_map = _fetch_connection_map()
        streams = []
        items = paths_data if isinstance(paths_data, list) else paths_data.get('items', {})

        if isinstance(items, list):
            for path_info in items:
                path_name = path_info.get('name', '')
                if not path_name:
                    continue
                with hidden_streams_lock:
                    if path_name in hidden_streams and path_info.get('ready'):
                        hidden_streams.discard(path_name)
                        logger.info(f"Stream {path_name} back online, un-hiding")
                    skip = path_name in hidden_streams
                if skip:
                    continue
                streams.append(_extract_stream_info(path_name, path_info, conn_map))
        else:
            for path_name, path_info in items.items():
                with hidden_streams_lock:
                    if path_name in hidden_streams and path_info.get('ready'):
                        hidden_streams.discard(path_name)
                    skip = path_name in hidden_streams
                if skip:
                    continue
                streams.append(_extract_stream_info(path_name, path_info, conn_map))

        # Include pull streams that are actively reconnecting but not yet visible in MediaMTX.
        # This keeps the stream card present in the dashboard between reconnect attempts.
        seen = {s['name'] for s in streams}
        with pull_stream_lock:
            for name, cfg in list(pull_stream_configs.items()):
                if name not in seen:
                    streams.append({
                        'name': name,
                        'ready': False,
                        'pullStatus': 'reconnecting',
                        'retryCount': cfg.get('retry_count', 0),
                        'numReaders': 0,
                        'bytesReceived': 0,
                        'bytesSent': 0,
                        'recording': False,
                        'protocol': 'RTSP Pull',
                        'sourceAddress': None,
                        'sourceUrl': cfg.get('source_url', ''),
                        'lastDataTime': None,
                    })

        return jsonify(streams)

    except Exception as e:
        logger.error(f"Error listing streams: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/paths', methods=['GET'])
def list_stream_paths():
    """List all stream path names (simple list of strings)"""
    try:
        paths_data = mediamtx.list_paths()
        if not paths_data:
            return jsonify([])

        items = paths_data if isinstance(paths_data, list) else paths_data.get('items', {})

        if isinstance(items, list):
            path_names = [p.get('name', '') for p in items if p.get('name')]
        else:
            path_names = list(items.keys())

        return jsonify(path_names)

    except Exception as e:
        logger.error(f"Error listing stream paths: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>', methods=['POST'])
def create_stream(stream_name):
    """Create a persistent stream path in MediaMTX.
    
    Registers the path in MediaMTX config so it survives publisher
    disconnects.  Without this, paths only exist while a source is
    actively connected.
    """
    try:
        if not stream_name or not stream_name.strip():
            return jsonify({'error': 'Stream name required'}), 400

        # Build path config from request body (defaults to publisher source)
        body = request.get_json(silent=True) or {}
        path_config = {
            'source': body.get('source', 'publisher'),
            'overridePublisher': True,
        }

        if not mediamtx.add_path(stream_name, path_config):
            return jsonify({'error': f'Failed to register path in MediaMTX'}), 409

        # Un-hide in case it was previously deleted
        hidden_streams.discard(stream_name)

        logger.info(f"Created persistent stream path: {stream_name}")
        broadcast('stream_created', {'name': stream_name})
        return jsonify({'success': True, 'message': f'Stream {stream_name} created'})

    except Exception as e:
        logger.error(f"Error creating stream: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>', methods=['GET'])
def get_stream(stream_name):
    """Get specific stream details"""
    try:
        path_info = mediamtx.get_path(stream_name)

        if not path_info:
            return jsonify({'error': f'Stream {stream_name} not found'}), 404

        logger.debug(f"MediaMTX path_info keys: {list(path_info.keys())}")

        result = _extract_stream_info(stream_name, path_info)
        result['pulling'] = stream_name in active_pull_streams

        # Add recording info if active
        if stream_name in active_recordings:
            rec = active_recordings[stream_name]
            start_time = rec.get('startTime')
            result['recordingInfo'] = {
                'startTime': start_time.isoformat() if start_time else None,
                'duration': (datetime.now(timezone.utc) - start_time.replace(tzinfo=timezone.utc)).total_seconds() if start_time else 0,
                'outputFile': rec.get('file')
            }

        # Add pull stream info if active
        if stream_name in pull_stream_configs:
            config = pull_stream_configs[stream_name]
            result['pullInfo'] = {
                'sourceUrl': config['source_url'],
                'retryCount': config['retry_count'],
                'uptime': time.time() - config['start_time']
            }

        return jsonify(result)

    except Exception as e:
        logger.error(f"Error getting stream {stream_name}: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/viewers', methods=['GET'])
def list_stream_viewers(stream_name):
    """List all current viewers (readers) for a stream with their IP addresses."""
    try:
        path_info = mediamtx.get_path(stream_name)
        if not path_info:
            return jsonify([])

        readers = path_info.get('readers', []) or []
        if not readers:
            return jsonify([])

        # Readers from the path endpoint only have {type, id} — no remoteAddr.
        # Fetch the full connection list to resolve IPs.
        conn_map = _fetch_connection_map()

        viewers = []
        for reader in readers:
            if not isinstance(reader, dict):
                continue
            conn_type = reader.get('type', '')
            conn_id = reader.get('id', '')
            # Map source type to kick endpoint prefix
            kick_endpoint = {
                'srtConn': 'srtconns',
                'rtspSession': 'rtspsessions',
                'rtmpConn': 'rtmpconns',
                'webRTCSession': 'webrtcsessions',
            }.get(conn_type, '')
            ip = conn_map.get(conn_id, 'unknown')
            blocked = ip in _blocked_ips if ip != 'unknown' else False
            viewers.append({
                'id': conn_id,
                'connType': kick_endpoint,
                'protocol': _SOURCE_TYPE_LABELS.get(conn_type, conn_type or 'Unknown'),
                'ip': ip,
                'blocked': blocked,
            })
        return jsonify(viewers)

    except Exception as e:
        logger.error(f"Error listing viewers for {stream_name}: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/viewers/<conn_type>/<conn_id>/block', methods=['POST'])
def block_viewer(stream_name, conn_type, conn_id):
    """Block a viewer: kick the connection and add their IP to the persistent blocklist."""
    # Validate conn_type to avoid SSRF via crafted endpoint strings
    allowed_types = {'srtconns', 'rtspsessions', 'rtmpconns', 'webrtcsessions'}
    if conn_type not in allowed_types:
        return jsonify({'error': 'Invalid connection type'}), 400
    try:
        # Resolve IP before kicking (connection disappears after kick)
        conn_map = _fetch_connection_map()
        ip = conn_map.get(conn_id, None)

        success = mediamtx.kick_connection(conn_type, conn_id)
        if success:
            if ip and ip != 'unknown':
                _block_ip(ip)
                logger.info(f"Blocked viewer {conn_id} ({conn_type}) IP {ip} from {stream_name}")
                return jsonify({'success': True, 'blocked_ip': ip})
            else:
                logger.warning(f"Kicked viewer {conn_id} but could not resolve IP for blocklist")
                return jsonify({'success': True, 'blocked_ip': None})
        return jsonify({'error': 'Failed to block viewer'}), 500
    except Exception as e:
        logger.error(f"Error blocking viewer {conn_id}: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/blocked-ips', methods=['GET'])
def list_blocked_ips():
    """Return the list of all blocked IPs."""
    return jsonify(sorted(_blocked_ips))


@streams_bp.route('/api/blocked-ips/<path:ip>', methods=['DELETE'])
def unblock_ip_endpoint(ip):
    """Remove an IP from the blocklist."""
    if ip not in _blocked_ips:
        return jsonify({'error': 'IP not in blocklist'}), 404
    _unblock_ip(ip)
    return jsonify({'success': True})


@streams_bp.route('/api/streams/<path:stream_name>/stop', methods=['POST'])
def stop_stream(stream_name):
    """Stop a stream by kicking all connections and stopping test publishers"""
    try:
        stopped_components, kicked_count = _stop_stream_components(stream_name, remove_pull_config=False)

        message = f"Stream {stream_name} stopped"
        if stopped_components:
            message += f" (stopped: {', '.join(stopped_components)})"
        if kicked_count > 0:
            message += f" (kicked {kicked_count} connections)"

        logger.info(message)
        broadcast('stream_stopped', {'name': stream_name, 'connections': kicked_count, 'components': stopped_components})

        return jsonify({
            'success': True,
            'message': message,
            'connectionsKicked': kicked_count,
            'stoppedComponents': stopped_components
        })

    except Exception as e:
        logger.error(f"Error stopping stream: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>', methods=['DELETE'])
def delete_stream(stream_name):
    """Delete a stream and stop any active recording, publishers, and connections"""
    try:
        stopped_components, kicked_count = _stop_stream_components(stream_name, remove_pull_config=True)

        message = f"Stream {stream_name} deleted"
        if stopped_components:
            message += f" (stopped: {', '.join(stopped_components)})"
        if kicked_count > 0:
            message += f" (kicked {kicked_count} connections)"

        logger.info(message)
        broadcast('stream_deleted', {'name': stream_name, 'components': stopped_components, 'connections': kicked_count})
        return jsonify({'success': True, 'message': message})

    except Exception as e:
        logger.error(f"Error deleting stream: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/pull', methods=['POST'])
def start_pull_stream(stream_name):
    """
    Start a pull stream (re-publish external RTSP/SRT to MediaMTX)

    Request body:
        {
            "url": "rtsp://..." OR "sourceUrl": "rtsp://...",
            "username": "optional",
            "password": "optional"
        }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body required'}), 400

        source_url = data.get('url') or data.get('sourceUrl')
        if not source_url:
            return jsonify({'error': 'URL required in request body (url or sourceUrl)'}), 400

        # Validate source URL protocol
        import urllib.parse
        try:
            parsed = urllib.parse.urlparse(source_url)
        except Exception:
            return jsonify({'error': 'Malformed URL'}), 400
        if parsed.scheme not in ('rtsp', 'rtsps', 'srt', 'http', 'https'):
            return jsonify({'error': f'Unsupported protocol: {parsed.scheme}. Allowed: rtsp, rtsps, srt, http, https'}), 400
        if not parsed.netloc:
            return jsonify({'error': 'URL must include a host'}), 400

        username = data.get('username', '')
        password = data.get('password', '')

        with pull_stream_lock:
            if stream_name in active_pull_streams:
                return jsonify({'error': f'Pull stream {stream_name} already active'}), 400

        _start_pull_impl(stream_name, source_url, username, password)

        broadcast('pull_stream_started', {'name': stream_name, 'source': source_url})

        return jsonify({
            'success': True,
            'message': f'Pull stream started for {stream_name}',
            'source': source_url
        })

    except Exception as e:
        logger.error(f"Error starting pull stream: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/stop-pull', methods=['POST'])
def stop_pull_stream(stream_name):
    """Stop a pull stream"""
    try:
        with pull_stream_lock:
            if stream_name not in active_pull_streams:
                return jsonify({'error': f'Pull stream {stream_name} not active'}), 404

            process = active_pull_streams[stream_name]

        _terminate_process(process)

        with pull_stream_lock:
            if stream_name in active_pull_streams:
                del active_pull_streams[stream_name]
            if stream_name in pull_stream_configs:
                pull_stream_configs[stream_name]['auto_retry'] = False
                del pull_stream_configs[stream_name]

        _remove_pull_source(stream_name)
        logger.info(f"Pull stream stopped: {stream_name}")
        _externally_stopped.add(stream_name)
        broadcast('pull_stream_stopped', {'name': stream_name})

        return jsonify({
            'success': True,
            'message': f'Pull stream stopped for {stream_name}'
        })

    except Exception as e:
        logger.error(f"Error stopping pull stream: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/pull-status', methods=['GET'])
def get_stream_pull_status(stream_name):
    """Get pull stream status for specific stream"""
    try:
        if stream_name not in pull_stream_configs:
            return jsonify({'error': f'Pull stream {stream_name} not found'}), 404

        config = pull_stream_configs[stream_name]
        is_active = stream_name in active_pull_streams

        return jsonify({
            'name': stream_name,
            'active': is_active,
            'sourceUrl': config['source_url'],
            'retryCount': config['retry_count'],
            'autoRetry': config.get('auto_retry', True),
            'uptime': time.time() - config['start_time'] if is_active else 0
        })

    except Exception as e:
        logger.error(f"Error getting pull status for {stream_name}: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/pull-status', methods=['GET'])
def get_all_pull_status():
    """Get all pull stream statuses"""
    try:
        statuses = []

        for stream_name, config in list(pull_stream_configs.items()):
            is_active = stream_name in active_pull_streams
            statuses.append({
                'name': stream_name,
                'active': is_active,
                'sourceUrl': config['source_url'],
                'retryCount': config['retry_count'],
                'autoRetry': config.get('auto_retry', True),
                'uptime': time.time() - config['start_time'] if is_active else 0
            })

        return jsonify(statuses)

    except Exception as e:
        logger.error(f"Error getting pull statuses: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/recording-status', methods=['GET'])
def get_stream_recording_status(stream_name):
    """Get recording status for specific stream"""
    try:
        if stream_name not in active_recordings:
            return jsonify({
                'name': stream_name,
                'recording': False
            })

        rec = active_recordings[stream_name]
        start_time = rec.get('startTime')
        now = datetime.now(timezone.utc)
        return jsonify({
            'name': stream_name,
            'recording': True,
            'startTime': start_time.isoformat() if start_time else None,
            'duration': (now - start_time.replace(tzinfo=timezone.utc)).total_seconds() if start_time else 0,
            'outputFile': rec.get('file'),
            'codec': rec.get('codec', 'unknown')
        })

    except Exception as e:
        logger.error(f"Error getting recording status for {stream_name}: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/recording-status', methods=['GET'])
def get_recording_status():
    """Get recording status for all streams (compatibility endpoint for client.js)"""
    status = {}
    now = datetime.now(timezone.utc)
    for stream_name, recording in list(active_recordings.items()):
        start_time = recording.get('startTime')
        status[stream_name] = {
            'recording': True,
            'startTime': start_time.isoformat() if start_time else None,
            'duration': int((now - start_time.replace(tzinfo=timezone.utc)).total_seconds()) if start_time else 0,
            'outputFile': recording.get('file', ''),
            'codec': recording.get('codec', 'unknown')
        }
    return jsonify(status)


@streams_bp.route('/api/post-processing/status', methods=['GET'])
def get_post_processing_status():
    """Get status of post-processing queue"""
    try:
        queue_items = []

        for file_path, item in post_processing_queue.items():
            queue_items.append({
                'file': Path(file_path).name,
                'streamName': item.get('streamName'),
                'status': item.get('status'),
                'queuedTime': item.get('queuedTime'),
                'completedTime': item.get('completedTime'),
                'failedTime': item.get('failedTime'),
                'error': item.get('error')
            })

        return jsonify({
            'isProcessing': False,
            'queueLength': len(post_processing_queue),
            'queue': queue_items
        })

    except Exception as e:
        logger.error(f"Error getting post-processing status: {e}")
        return jsonify({'error': str(e)}), 500


@streams_bp.route('/api/streams/<path:stream_name>/standby', methods=['DELETE'])
def remove_standby_stream(stream_name):
    """Remove a stream from standby tracking"""
    try:
        from app.services.standby import standby_manager
        removed = standby_manager.remove_stream(stream_name)
        if removed:
            return jsonify({'message': f'{stream_name} removed from standby'})
        return jsonify({'error': 'Stream not found in standby'}), 404
    except Exception as e:
        logger.error(f"Error removing standby stream: {e}")
        return jsonify({'error': str(e)}), 500
