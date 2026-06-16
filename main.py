"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

Main entry point for TAK Video Restreamer
"""
import sys
import signal
import threading
import time
import logging
import requests
from datetime import datetime, timezone
import subprocess
import os

from app import create_app
from app.config import PORT, MEDIAMTX_API_URL, MEDIAMTX_RTSP_URL, STREAMS_DIR, SRT_BUFFER_AVAILABLE
from app.state import (
    active_recordings, active_pull_streams, thumbnail_executor, post_process_executor,
    known_streams, get_srt_buffer_manager, recording_lock
)
from app.utils.codec_detection import detect_stream_codec
from app.websocket.broadcast import broadcast
from app.services.standby import standby_manager
from app.services.cleanup import start_cleanup_service
from app.services.mediamtx import MediaMTXClient

logger = logging.getLogger(__name__)

# Global references for cleanup
app = None
socketio = None


def cleanup_on_exit(signum, frame):
    """Cleanup handler for graceful shutdown"""
    logger.info("=" * 80)
    logger.info("Shutting down gracefully...")

    # Stop all active recordings
    for stream_name, recording in list(active_recordings.items()):
        try:
            recording['process'].terminate()
            recording['process'].wait(timeout=3)
            logger.info(f"Stopped recording: {stream_name}")
        except Exception as e:
            logger.error(f"Error stopping recording {stream_name}: {e}")

    # Stop all pull streams
    for stream_name, process in list(active_pull_streams.items()):
        try:
            process.terminate()
            process.wait(timeout=3)
            logger.info(f"Stopped pull stream: {stream_name}")
        except Exception as e:
            logger.error(f"Error stopping pull stream {stream_name}: {e}")

    # Shutdown thread pools
    logger.info("Shutting down thread pools...")
    try:
        thumbnail_executor.shutdown(wait=True, cancel_futures=True)
        logger.info("Thumbnail executor shut down")
    except Exception as e:
        logger.error(f"Error shutting down thumbnail executor: {e}")

    try:
        post_process_executor.shutdown(wait=True, cancel_futures=True)
        logger.info("Post-process executor shut down")
    except Exception as e:
        logger.error(f"Error shutting down post-process executor: {e}")

    logger.info("Flask Media Server Stopped")
    logger.info("=" * 80)
    sys.exit(0)


def monitor_streams_for_auto_record():
    """Background thread that monitors for new inbound streams and auto-starts recording"""
    import app.state as app_state
    from app.api.settings import server_settings

    logger.info("Stream monitor thread started")

    _blocklist_mtx = MediaMTXClient(MEDIAMTX_API_URL)

    while True:
        try:
            # Check if health monitoring / auto-record is enabled
            if not server_settings.get('health_check_enabled', True):
                time.sleep(2)
                continue

            if app_state.auto_record_enabled:
                # Get current streams from MediaMTX
                response = requests.get(f'{MEDIAMTX_API_URL}/v3/paths/list', timeout=5)
                if response.status_code == 200:
                    paths_data = response.json()
                    items = paths_data if isinstance(paths_data, list) else paths_data.get('items', {})

                    # Handle both list and dict responses
                    if isinstance(items, list):
                        current_streams = [p.get('name', '') for p in items if p.get('name') and p.get('ready', False)]
                    else:
                        current_streams = [name for name, info in items.items() if info.get('ready', False)]

                    # Check for new streams
                    for stream_name in current_streams:
                        # Track in standby manager
                        standby_manager.stream_seen(stream_name)

                        if stream_name not in known_streams:
                            logger.info(f"New inbound stream detected: {stream_name}")
                            known_streams[stream_name] = True

                            # Enable SRT buffering for drone/UAS streams if configured
                            if SRT_BUFFER_AVAILABLE and server_settings.get('srt_buffer_enabled', False):
                                if any(keyword in stream_name.lower() for keyword in ['drone', 'uas', 'tak']):
                                    try:
                                        srt_manager = get_srt_buffer_manager()
                                        if srt_manager and srt_manager.add_stream(stream_name):
                                            logger.info(f"Enabled SRT buffering with auto-reconnect for: {stream_name}")
                                    except Exception as e:
                                        logger.error(f"Failed to enable SRT buffering for {stream_name}: {e}")

                            # Start recording if not already recording
                            if stream_name not in active_recordings:
                                logger.info(f"Auto-starting recording for stream: {stream_name}")
                                try:
                                    # Build RTSP URL for the stream
                                    rtsp_url = f"{MEDIAMTX_RTSP_URL.rstrip('/')}/{stream_name}"

                                    # Detect codec and start recording
                                    stream_info = detect_stream_codec(rtsp_url)
                                    if stream_info:
                                        now = datetime.now(timezone.utc)
                                        timestamp = now.strftime('%Y%m%d_%H%M%S')
                                        filename = f"{stream_name}_{timestamp}.mov"
                                        output_path = os.path.join(STREAMS_DIR, filename)

                                        has_audio = stream_info.get('has_audio', False)
                                        has_data = stream_info.get('has_data', False)

                                        # Build FFmpeg command
                                        ffmpeg_args = [
                                            'ffmpeg',
                                            '-i', rtsp_url,
                                            '-map', '0:v',
                                            '-map', '0:a?',
                                            '-map', '0:d?',
                                            '-c:v', 'libx264',
                                            '-preset', 'ultrafast',
                                            '-r', '29.97',
                                        ]

                                        if has_audio:
                                            ffmpeg_args.extend(['-c:a', 'aac'])

                                        if has_data:
                                            ffmpeg_args.extend(['-c:d', 'copy'])
                                            logger.info(f"Recording with KLV preservation enabled for {stream_name}")

                                        ffmpeg_args.extend([
                                            '-movflags', '+faststart',
                                            '-y',
                                            output_path
                                        ])

                                        # Start FFmpeg process
                                        # stderr→DEVNULL: PIPE causes deadlock after ~200s
                                        # (64KB OS buffer fills with progress lines, FFmpeg blocks)
                                        process = subprocess.Popen(
                                            ffmpeg_args,
                                            stdout=subprocess.DEVNULL,
                                            stderr=subprocess.DEVNULL,
                                        )

                                        # Store recording info
                                        with recording_lock:
                                            active_recordings[stream_name] = {
                                                'process': process,
                                                'filename': filename,
                                                'output_path': output_path,
                                                'start_time': now.isoformat(),
                                                'auto_started': True
                                            }

                                        logger.info(f"Auto-record started for {stream_name}: {filename}")
                                        broadcast('recording_started', {
                                            'stream': stream_name,
                                            'filename': filename,
                                            'hasKlv': has_data,
                                            'auto_started': True
                                        })

                                except Exception as e:
                                    logger.error(f"Error auto-starting recording for {stream_name}: {e}")

                    # Clean up known_streams for streams that no longer exist
                    for stream_name in list(known_streams.keys()):
                        if stream_name not in current_streams:
                            # Mark as standby (gone)
                            standby_manager.stream_gone(stream_name)
                            del known_streams[stream_name]

        except Exception as e:
            logger.error(f"Error in stream monitor thread: {e}")

        # Enforce IP blocklist — kick any blocked IPs that have reconnected
        try:
            from app.api.streams import get_blocked_ips
            blocked = get_blocked_ips()
            if blocked:

                conns = _blocklist_mtx.list_connections()
                for conn in conns:
                    raw_addr = conn.get('remoteAddr', '')
                    ip = raw_addr.rsplit(':', 1)[0] if raw_addr else ''
                    if ip in blocked:
                        conn_type = conn.get('_conn_type', '')
                        conn_id = conn.get('id', '')
                        if conn_type and conn_id:
                            _blocklist_mtx.kick_connection(conn_type, conn_id)
                            logger.info(f"Blocklist enforcement: kicked {ip} ({conn_type}/{conn_id})")
        except Exception as e:
            logger.debug(f"Blocklist enforcement error: {e}")

        # Check every 2 seconds
        time.sleep(2)


# Create Flask app at module level for Gunicorn
app = create_app()
socketio = app.socketio

# Load persisted settings and apply to MediaMTX (MTU etc.)
from app.api.settings import load_and_apply_server_settings
load_and_apply_server_settings()

if __name__ == '__main__':
    # Register signal handlers
    signal.signal(signal.SIGINT, cleanup_on_exit)
    signal.signal(signal.SIGTERM, cleanup_on_exit)

    logger.info(f"Starting TAK Video Restreamer on port {PORT}")
    logger.info(f"MediaMTX API: {MEDIAMTX_API_URL}")
    logger.info(f"Streams directory: {STREAMS_DIR}")

    # Start auto-record monitor thread
    monitor_thread = threading.Thread(target=monitor_streams_for_auto_record, daemon=True)
    monitor_thread.start()
    logger.info("Auto-record monitor thread started")

    # Start auto-cleanup service
    start_cleanup_service()

    # Start the server
    socketio.run(app, host='0.0.0.0', port=PORT, debug=False, allow_unsafe_werkzeug=True)
else:
    # When running under Gunicorn, start the monitor thread
    monitor_thread = threading.Thread(target=monitor_streams_for_auto_record, daemon=True)
    monitor_thread.start()
    logger.info("Auto-record monitor thread started (Gunicorn mode)")
    start_cleanup_service()
