"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

Recordings API Blueprint - Recording management endpoints
"""
from flask import Blueprint, request, jsonify, send_file
from datetime import datetime, timezone
import json
import subprocess
import threading
import time
import os
import shutil
import re
from pathlib import Path
import logging

from app.config import STREAMS_DIR, SHARED_VIDEOS_DIR
from app.state import active_recordings, thumbnail_executor, post_processing_queue, recording_lock
from app.api.settings import server_settings
from app.utils.codec_detection import detect_stream_codec, analyze_recording
from app.utils.thumbnail import generate_thumbnail
from app.websocket.broadcast import broadcast

logger = logging.getLogger(__name__)

recordings_bp = Blueprint('recordings', __name__)

# Stream name validation regex - only safe characters
STREAM_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')


def validate_stream_name(stream_name: str) -> bool:
    """Validate stream name contains only safe characters"""
    return bool(STREAM_NAME_PATTERN.match(stream_name)) and len(stream_name) <= 64


def validate_filename(filename: str) -> bool:
    """Validate filename for path traversal attacks"""
    if not filename:
        return False
    # Block path traversal
    if '..' in filename or '/' in filename or '\\' in filename:
        return False
    # Must have a valid extension
    if not filename.lower().endswith(('.mp4', '.mov', '.ts', '.mxf', '.mpg', '.mpeg', '.mkv')):
        return False
    return True


def validate_path_within_streams_dir(file_path: str) -> bool:
    """Ensure resolved path is within STREAMS_DIR"""
    try:
        resolved = Path(file_path).resolve()
        streams_dir_resolved = Path(STREAMS_DIR).resolve()
        return resolved.is_relative_to(streams_dir_resolved)
    except (ValueError, RuntimeError):
        return False


def sanitize_metadata(value: str) -> str:
    """Sanitize metadata values to prevent command injection"""
    if not value:
        return ""
    # Remove potentially dangerous characters for FFmpeg metadata
    sanitized = re.sub(r'[;\'"\\`$]', '', str(value))
    return sanitized[:256]  # Limit length


@recordings_bp.route('/api/streams/<path:stream_name>/record', methods=['POST'])
def start_recording(stream_name):
    """
    Start recording a stream with CFR 30fps re-encoding for timecode sync
    Supports RTSP and SRT protocols
    """
    try:
        # Validate stream name
        if not validate_stream_name(stream_name):
            return jsonify({'error': 'Invalid stream name. Only alphanumeric, hyphens, and underscores allowed.'}), 400
        
        # Check for copy mode (skip re-encoding for H.264 sources)
        request_data = request.get_json(silent=True) or {}
        force_copy = request_data.get('copyMode', False)
        
        # Use lock to prevent race condition during recording state check/set
        with recording_lock:
            if stream_name in active_recordings:
                return jsonify({'error': f'Stream {stream_name} is already being recorded'}), 400
            
            # Reserve the recording slot to prevent race condition
            active_recordings[stream_name] = {'status': 'starting'}
        
        # Try RTSP first, then SRT
        rtsp_url = f'rtsp://localhost:8554/{stream_name}'
        srt_url = f'srt://localhost:8890?streamid=read:{stream_name}'
        
        # Notify clients that codec detection has started
        broadcast('codec_detecting', {'name': stream_name, 'status': 'probing'})
        
        # Detect codec - try RTSP first
        stream_info = detect_stream_codec(rtsp_url, timeout=10)
        stream_url = rtsp_url
        input_options = ['-rtsp_transport', server_settings.get('rtsp_transport', 'tcp')]
        
        # If RTSP fails, try SRT
        if not stream_info:
            logger.info(f"RTSP detection failed, trying SRT for {stream_name}")
            stream_info = detect_stream_codec(srt_url, timeout=10)
            if stream_info:
                stream_url = srt_url
                input_options = []  # No special options for SRT
        
        if not stream_info:
            # Clean up reserved slot on failure
            with recording_lock:
                if stream_name in active_recordings:
                    del active_recordings[stream_name]
            return jsonify({'error': 'Failed to detect stream codec - stream may not be active or not accessible via RTSP or SRT'}), 400
        
        logger.info(f"Recording {stream_name} from {stream_info.get('protocol', 'unknown').upper()} protocol")
        
        # Create stream directory
        stream_dir = os.path.join(STREAMS_DIR, stream_name)
        os.makedirs(stream_dir, exist_ok=True)
        
        # Generate filename with UTC timestamp
        now = datetime.now(timezone.utc)
        timestamp = now.strftime('%Y-%m-%dT%H-%M-%S-%f')[:-3] + 'Z'
        recording_file = os.path.join(stream_dir, f'recording-{timestamp}.mov')
        
        # Calculate UTC timecode at 29.97fps (matches UTC with drop-frame)
        hours = now.hour
        minutes = now.minute
        seconds = now.second
        frames = int((now.microsecond / 1000000.0) * 29.97)
        timecode_ffmpeg = f'{hours:02d}:{minutes:02d}:{seconds:02d}:{frames:02d}'
        timecode_metadata = f'{hours:02d}:{minutes:02d}:{seconds:02d}.{now.microsecond // 1000:03d}'
        
        # Build FFmpeg arguments
        ffmpeg_args = ['ffmpeg']
        
        # Add protocol-specific input options
        ffmpeg_args.extend(input_options)

        # Add connection timeout
        timeout_us = server_settings.get('connection_timeout', 5000000)
        ffmpeg_args.extend(['-timeout', str(timeout_us)])

        # Add FFmpeg reconnect options — only valid for HTTP/HTTPS sources, not RTSP/SRT
        if server_settings.get('enable_ffmpeg_reconnect', True) and stream_url.startswith('http'):
            ffmpeg_args.extend([
                '-reconnect', '1',
                '-reconnect_streamed', '1',
                '-reconnect_delay_max', '5',
            ])

        ffmpeg_args.extend([
            '-i', stream_url,
            '-err_detect', 'ignore_err',
            '-fflags', '+genpts+discardcorrupt',
            '-flags', 'low_delay'
        ])
        
        # Map all streams including KLV data if present
        ffmpeg_args.extend([
            '-map', '0:v',   # Map video stream
            '-map', '0:a?',  # Map audio if present (optional)
            '-map', '0:d?'   # Map data streams (KLV) if present (optional)
        ])
        
        # Video encoding strategy:
        # Copy mode: use -c:v copy when source is H.264 or AV1 (much lower CPU, preserves original quality)
        # Re-encode mode: force CFR 29.97fps for proper drop-frame timecode (default for other codecs)
        _src_codec = stream_info.get('codec')
        use_copy = force_copy and _src_codec in ('h264', 'av1')
        
        if use_copy:
            ffmpeg_args.extend(['-c:v', 'copy'])
            logger.info(f"Recording with copy mode (no re-encoding) — source is {_src_codec}")
        else:
            # ALWAYS re-encode to force CFR 29.97fps for proper drop-frame timecode
            # VFR streams cause timecode drift, must convert to CFR
            ffmpeg_args.extend([
                '-c:v', 'libx264',
                '-preset', 'ultrafast',  # Fastest preset for real-time encoding
                '-tune', 'zerolatency',  # Optimize for streaming/recording
                '-crf', '23',  # Balanced quality (lower = better, 23 is default)
                '-r', '30000/1001',  # Force constant 29.97fps (NTSC drop-frame standard)
                '-vsync', 'cfr',  # Constant frame rate mode
                '-tag:v', 'avc1'
            ])
            logger.info(f"Recording with re-encoding (libx264 ultrafast) at CFR 29.97fps for drop-frame timecode sync")
        
        # Preserve data streams (KLV) by copying them
        if stream_info.get('has_data'):
            ffmpeg_args.extend(['-c:d', 'copy'])  # Copy data stream as-is
            logger.info(f"Recording with KLV preservation enabled")
        
        # Audio and metadata
        # Add recording start time in ISO 8601 UTC format
        recording_start_utc = now.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # Sanitize stream_name for use in metadata
        safe_stream_name = sanitize_metadata(stream_name)
        safe_codec = sanitize_metadata(stream_info.get('codec', 'unknown'))
        
        ffmpeg_args.extend([
            '-c:a', 'aac',
            '-b:a', '128k',
            '-ar', '48000',
            '-af', 'aresample=async=1000',  # Re-sync audio when VFR→CFR conversion shifts video timestamps
        ])
        
        # Timecode embedding (only when re-encoding — copy mode preserves original timestamps)
        if not use_copy:
            ffmpeg_args.extend([
                '-write_tmcd', '1',
                '-timecode', timecode_ffmpeg,
                '-video_track_timescale', '30000',  # Force non-drop-frame at 30fps
            ])
        
        # Output format: segmented or single file
        if server_settings.get('segmented_recording', False):
            seg_dur = server_settings.get('segment_duration', 600)
            # Segmented recording: produces recording-<timestamp>-000.mov, -001.mov, etc.
            segment_pattern = os.path.join(stream_dir, f'recording-{timestamp}-%03d.mov')
            ffmpeg_args.extend([
                '-f', 'segment',
                '-segment_time', str(seg_dur),
                '-reset_timestamps', '1',
                '-segment_format', 'mov',
                '-movflags', '+faststart+write_colr',
                '-metadata', f'title=Recording: {safe_stream_name}',
                '-metadata', f'date={now.strftime("%Y-%m-%d")}',
                '-metadata', f'creation_time={recording_start_utc}',
                '-metadata', f'comment=Recording started {recording_start_utc} UTC, Timecode: {timecode_metadata}',
                '-metadata', f'artist={safe_stream_name}',
                '-metadata', f'encoder=FlaskMediaServer ({safe_codec})',
                segment_pattern
            ])
            # Store segment pattern for listing later
            recording_file = segment_pattern
            logger.info(f"Segmented recording: {seg_dur}s per segment")
        else:
            ffmpeg_args.extend([
                '-f', 'mov',
                '-movflags', '+faststart+write_colr',
                '-metadata', f'title=Recording: {safe_stream_name}',
                '-metadata', f'date={now.strftime("%Y-%m-%d")}',
                '-metadata', f'creation_time={recording_start_utc}',
                '-metadata', f'timecode={timecode_metadata}',
                '-metadata', f'comment=Recording started {recording_start_utc} UTC, Timecode: {timecode_metadata}',
                '-metadata', f'artist={safe_stream_name}',
                '-metadata', f'encoder=FlaskMediaServer ({safe_codec})',
                recording_file
            ])
        
        logger.info(f"Starting recording: {stream_name} -> {Path(recording_file).name} TC={timecode_ffmpeg}")
        
        # Start FFmpeg process
        process = subprocess.Popen(
            ffmpeg_args,
            stdin=subprocess.PIPE,  # Allow sending 'q' for graceful shutdown
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Store recording info
        active_recordings[stream_name] = {
            'process': process,
            'file': recording_file,
            'startTime': now,
            'pid': process.pid,
            'timecode': timecode_ffmpeg,
            'timecodeMetadata': timecode_metadata,
            'codec': stream_info['codec']
        }
        
        # Start thread to monitor FFmpeg output
        def monitor_ffmpeg():
            stderr_output = []
            for line in process.stderr:
                stderr_output.append(line.decode('utf-8', errors='ignore'))
            
            return_code = process.wait()
            duration = int((datetime.now(timezone.utc) - now).total_seconds())
            
            if return_code != 0:
                logger.error(f"FFmpeg exited with code {return_code} for {stream_name} after {duration}s")
                logger.error(f"FFmpeg stderr: {''.join(stderr_output[-20:])}")
            else:
                logger.info(f"FFmpeg completed successfully for {stream_name} after {duration}s")
                # Log last few lines of stderr to understand why it stopped
                if stderr_output:
                    logger.info(f"FFmpeg final output: {''.join(stderr_output[-10:])}")
                # Generate thumbnail using thread pool executor
                thumbnail_executor.submit(generate_thumbnail, recording_file, stream_name)
            
            # Clean up only if not already stopped by user
            # The stop endpoint will handle cleanup properly
            if stream_name in active_recordings:
                recording = active_recordings[stream_name]
                # Only auto-cleanup if process exited on its own (not terminated by user)
                if recording.get('process') and recording['process'].poll() is not None:
                    logger.info(f"Auto-cleanup recording for {stream_name} (FFmpeg exited)")
                    del active_recordings[stream_name]
        
        threading.Thread(target=monitor_ffmpeg, daemon=True).start()

        # Start file size monitor thread if max_file_size_gb is configured
        max_size_gb = server_settings.get('max_file_size_gb', 10)
        def monitor_file_size():
            max_bytes = max_size_gb * (1024 ** 3)
            while process.poll() is None:
                time.sleep(10)
                try:
                    # For segmented recording, check total size of all segments
                    if server_settings.get('segmented_recording', False):
                        total = sum(
                            f.stat().st_size for f in Path(stream_dir).glob(f'recording-{timestamp}-*.mov')
                        )
                    else:
                        total = os.path.getsize(recording_file) if os.path.exists(recording_file) else 0
                    if total >= max_bytes:
                        logger.warning(f"Recording {stream_name} reached {total / (1024**3):.1f}GB limit ({max_size_gb}GB), stopping")
                        try:
                            process.stdin.write(b'q')
                            process.stdin.flush()
                        except Exception:
                            process.terminate()
                        broadcast('recording_size_limit', {'name': stream_name, 'size_gb': round(total / (1024**3), 2)})
                        break
                except Exception:
                    pass

        threading.Thread(target=monitor_file_size, daemon=True).start()
        
        broadcast('recording_started', {
            'name': stream_name,
            'file': recording_file,
            'timecode': timecode_ffmpeg,
            'hasKlv': stream_info.get('has_data', False)
        })
        
        return jsonify({
            'success': True,
            'message': f'Recording started for {stream_name}',
            'file': recording_file,
            'timecode': timecode_ffmpeg,
            'codec': stream_info['codec'],
            'hasKlv': stream_info.get('has_data', False),
            'copyMode': use_copy
        })
        
    except Exception as e:
        logger.error(f"Error starting recording: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/streams/<path:stream_name>/stop-record', methods=['POST'])
def stop_recording(stream_name):
    """Stop recording a stream"""
    
    try:
        if stream_name not in active_recordings:
            # May have already stopped via monitor thread
            logger.warning(f"Stream {stream_name} is not in active recordings (may have already stopped)")
            return jsonify({
                'success': True,
                'message': f'Recording already stopped for {stream_name}'
            })
        
        recording = active_recordings[stream_name]
        process = recording['process']
        recording_file = recording['file']
        start_time = recording['startTime']
        
        # Check if process is still running
        if process.poll() is None:
            # Process still running - send 'q' for graceful quit to ensure moov atom is written
            logger.info(f"Sending graceful quit signal to FFmpeg for {stream_name}")
            try:
                process.stdin.write(b'q')
                process.stdin.flush()
                process.stdin.close()
            except Exception as e:
                logger.warning(f"Could not send quit signal: {e}")
            
            try:
                # Give FFmpeg time to finish writing properly
                process.wait(timeout=10)
                logger.info(f"FFmpeg finished gracefully for {stream_name}")
            except subprocess.TimeoutExpired:
                logger.warning(f"FFmpeg didn't finish gracefully, forcing terminate for {stream_name}")
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    logger.warning(f"FFmpeg didn't terminate, forcing kill for {stream_name}")
                    process.kill()
                    process.wait()
        else:
            logger.info(f"FFmpeg already exited for {stream_name} (code: {process.returncode})")
        
        # Calculate duration
        duration = int((datetime.now(timezone.utc) - start_time).total_seconds())
        
        logger.info(f"Recording stopped: {stream_name}, duration: {duration}s")
        
        # Clean up active recordings FIRST to prevent duplicate stops
        del active_recordings[stream_name]
        
        # Analyze recording if file exists
        analysis = None
        if os.path.exists(recording_file):
            try:
                analysis = analyze_recording(recording_file)
            except Exception as e:
                logger.error(f"Error analyzing recording: {e}")
        
        # Queue for post-processing only if file exists
        if os.path.exists(recording_file) and os.path.getsize(recording_file) > 0:
            post_processing_queue[recording_file] = {
                'status': 'pending',
                'streamName': stream_name,
                'timecode': recording.get('timecode', '00:00:00:00'),
                'queuedTime': time.time()
            }
            
            # Import and start post-processing in background
            # NOTE: This would be moved to a service in production
            # For now, just mark as queued
            # threading.Thread(target=process_recording_queue, daemon=True).start()
        
        broadcast('recording_stopped', {
            'name': stream_name,
            'file': recording_file,
            'duration': duration,
            'analysis': analysis
        })

        # Copy recording file to shared directory
        if os.path.exists(recording_file) and os.path.getsize(recording_file) > 0 and os.path.isdir(SHARED_VIDEOS_DIR):
            logger.info(f"Writing video file to {SHARED_VIDEOS_DIR}...")
            shutil.copy(recording_file, SHARED_VIDEOS_DIR)
        else:
            if not os.path.exists(recording_file):
                logger.info(f"Not writing video file to {SHARED_VIDEOS_DIR} because no video file was found")
            elif os.path.getsize(recording_file) == 0:
                logger.info(f"Not writing video file to {SHARED_VIDEOS_DIR} because video file was empty")
            elif not os.path.isdir(SHARED_VIDEOS_DIR):
                logger.info(f"Not writing video file to {SHARED_VIDEOS_DIR} because SHARED_VIDEOS_DIR does not exist")
            else:
                logger.info(f"Not writing video file to {SHARED_VIDEOS_DIR}. "
                            f"os.path.exists(recording_file): {os.path.exists(recording_file)}, "
                            f"os.path.getsize(recording_file): {os.path.getsize(recording_file)}, "
                            f"os.path.isdir(SHARED_VIDEOS_DIR): {os.path.isdir(SHARED_VIDEOS_DIR)}")
        
        return jsonify({
            'success': True,
            'message': f'Recording stopped for {stream_name}',
            'file': recording_file,
            'duration': f'{duration} seconds',
            'analysis': analysis,
            'postProcessing': 'queued' if os.path.exists(recording_file) else 'skipped'
        })
        
    except KeyError as e:
        logger.error(f"KeyError stopping recording {stream_name}: {e}")
        # Clean up if exists
        if stream_name in active_recordings:
            del active_recordings[stream_name]
        return jsonify({'success': True, 'message': 'Recording stopped (partial cleanup)'})
    except Exception as e:
        logger.error(f"Error stopping recording {stream_name}: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings', methods=['GET'])
def list_recordings():
    """List all recorded files"""
    try:
        if not os.path.exists(STREAMS_DIR):
            return jsonify([])
        
        recordings = []
        
        for stream_name in os.listdir(STREAMS_DIR):
            stream_dir = os.path.join(STREAMS_DIR, stream_name)
            
            if not os.path.isdir(stream_dir):
                continue
            
            for filename in os.listdir(stream_dir):
                # Skip backup, temp, and test files
                if any(x in filename for x in ['_backup', '_temp', 'test_tmcd']):
                    continue
                
                # Only include video formats (mp4, mov, ts, mxf, mpg, mpeg, mkv)
                filename_lower = filename.lower()
                if not (filename_lower.endswith('.mp4') or filename_lower.endswith('.mov') or 
                        filename_lower.endswith('.ts') or filename_lower.endswith('.mxf') or
                        filename_lower.endswith('.mpg') or filename_lower.endswith('.mpeg') or
                        filename_lower.endswith('.mkv')):
                    continue
                
                file_path = os.path.join(stream_dir, filename)
                stats = os.stat(file_path)
                
                # Use file modification time for both created and modified
                created_date = datetime.fromtimestamp(stats.st_ctime)  # Creation time
                modified_date = datetime.fromtimestamp(stats.st_mtime)  # Modification time
                
                # Check for thumbnail
                thumbnail_path = file_path.rsplit('.', 1)[0] + '_thumb.jpg'
                has_thumbnail = os.path.exists(thumbnail_path)
                
                # Check if this file is currently being recorded
                is_recording = False
                for rec_name, rec_info in active_recordings.items():
                    if rec_info.get('file') == file_path:
                        is_recording = True
                        break
                
                recordings.append({
                    'stream': stream_name,
                    'filename': filename,
                    'size': stats.st_size,
                    'created': created_date.isoformat(),
                    'modified': modified_date.isoformat(),
                    'status': 'recording' if is_recording else 'finalized',
                    'hasThumbnail': has_thumbnail,
                    'url': f'/api/recordings/{stream_name}/{filename}',
                    'thumbnailUrl': f'/api/recordings/{stream_name}/{filename}/thumbnail' if has_thumbnail else None
                })
        
        # Sort by created date, newest first
        recordings.sort(key=lambda x: x['created'], reverse=True)
        
        return jsonify(recordings)
        
    except Exception as e:
        logger.error(f"Error listing recordings: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings/<path:stream_name>/<filename>', methods=['GET'])
def download_recording(stream_name, filename):
    """Download a recording file"""
    try:
        # Validate stream name and filename for security
        if not validate_stream_name(stream_name):
            return jsonify({'error': 'Invalid stream name'}), 400
        if not validate_filename(filename):
            return jsonify({'error': 'Invalid filename'}), 400
        
        file_path = os.path.join(STREAMS_DIR, stream_name, filename)
        
        # Ensure path is within STREAMS_DIR (defense in depth)
        if not validate_path_within_streams_dir(file_path):
            logger.warning(f"Path traversal attempt blocked: {file_path}")
            return jsonify({'error': 'Access denied'}), 403
        
        if not os.path.exists(file_path):
            return jsonify({'error': 'File not found'}), 404
        
        return send_file(file_path, as_attachment=True)
        
    except Exception as e:
        logger.error(f"Error downloading recording: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings/<path:stream_name>/<filename>/thumbnail', methods=['GET'])
def get_thumbnail(stream_name, filename):
    """Get thumbnail for a recording"""
    try:
        # Validate stream name and filename
        if not validate_stream_name(stream_name):
            return jsonify({'error': 'Invalid stream name'}), 400
        if '..' in filename or '/' in filename or '\\' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        
        thumbnail_path = os.path.join(STREAMS_DIR, stream_name, filename.rsplit('.', 1)[0] + '_thumb.jpg')
        
        # Ensure path is within STREAMS_DIR
        if not validate_path_within_streams_dir(thumbnail_path):
            return jsonify({'error': 'Access denied'}), 403
        
        if not os.path.exists(thumbnail_path):
            return jsonify({'error': 'Thumbnail not found'}), 404
        
        response = send_file(thumbnail_path, mimetype='image/jpeg')
        response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
        response.headers['Expires'] = '0'
        return response
        
    except Exception as e:
        logger.error(f"Error getting thumbnail: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings/<path:stream_name>/<filename>/generate-thumbnail', methods=['POST'])
def trigger_thumbnail_generation(stream_name, filename):
    """Generate thumbnail for a recording"""
    try:
        # Validate filename
        if '..' in filename or '/' in filename or '\\' in filename:
            return jsonify({'error': 'Invalid filename'}), 400
        
        video_path = os.path.join(STREAMS_DIR, stream_name, filename)
        
        if not os.path.exists(video_path):
            return jsonify({'error': 'Video file not found'}), 404
        
        # Generate thumbnail using existing function
        thumbnail_path = generate_thumbnail(video_path, stream_name)
        
        if thumbnail_path and os.path.exists(thumbnail_path):
            return jsonify({
                'success': True,
                'thumbnail': f'/api/recordings/{stream_name}/{filename}/thumbnail'
            })
        else:
            return jsonify({'error': 'Failed to generate thumbnail'}), 500
        
    except Exception as e:
        logger.error(f"Error generating thumbnail: {e}")
        return jsonify({'error': str(e)}), 500


def delete_sidecar_files(stream_name: str, base_name: str) -> None:
    """Delete KLV and other sidecar files for a recording"""
    sidecar_suffixes = ['_extracted_klv.json', '_extracted_klv_raw.json']
    for suffix in sidecar_suffixes:
        sidecar_path = os.path.join(STREAMS_DIR, stream_name, base_name + suffix)
        if validate_path_within_streams_dir(sidecar_path) and os.path.exists(sidecar_path):
            os.remove(sidecar_path)
            logger.info(f"Deleted sidecar file: {sidecar_path}")


@recordings_bp.route('/api/recordings/<path:stream_name>/<filename>', methods=['DELETE'])
def delete_recording(stream_name, filename):
    """Delete a recording and its thumbnail"""
    try:
        # Validate stream name and filename
        if not validate_stream_name(stream_name):
            return jsonify({'error': 'Invalid stream name'}), 400
        if not validate_filename(filename):
            return jsonify({'error': 'Invalid filename'}), 400
        
        video_path = os.path.join(STREAMS_DIR, stream_name, filename)
        
        # Ensure path is within STREAMS_DIR
        if not validate_path_within_streams_dir(video_path):
            logger.warning(f"Path traversal attempt blocked in delete: {video_path}")
            return jsonify({'error': 'Access denied'}), 403
        
        if not os.path.exists(video_path):
            return jsonify({'error': 'Video file not found'}), 404
        
        # Delete video file
        os.remove(video_path)
        logger.info(f"Deleted recording: {video_path}")
        
        # Delete thumbnail if exists
        base_name = filename.rsplit('.', 1)[0]
        thumbnail_path = os.path.join(STREAMS_DIR, stream_name, base_name + '_thumb.jpg')
        if validate_path_within_streams_dir(thumbnail_path) and os.path.exists(thumbnail_path):
            os.remove(thumbnail_path)
            logger.info(f"Deleted thumbnail: {thumbnail_path}")

        # Delete KLV sidecar files
        delete_sidecar_files(stream_name, base_name)
        
        return jsonify({'success': True, 'message': 'Recording deleted'})
        
    except Exception as e:
        logger.error(f"Error deleting recording: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings/bulk-delete', methods=['POST'])
def bulk_delete_recordings():
    """Delete multiple recordings at once"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body required'}), 400
            
        recordings = data.get('recordings', [])
        
        if not recordings or not isinstance(recordings, list):
            return jsonify({'error': 'Invalid request: recordings array required'}), 400
        
        deleted_count = 0
        errors = []
        
        for recording in recordings:
            stream_name = recording.get('stream')
            filename = recording.get('filename')
            
            if not stream_name or not filename:
                errors.append(f'Missing stream or filename in recording entry')
                continue
            
            # Validate stream name and filename
            if not validate_stream_name(stream_name):
                errors.append(f'Invalid stream name: {stream_name}')
                continue
            if not validate_filename(filename):
                errors.append(f'Invalid filename: {filename}')
                continue
            
            video_path = os.path.join(STREAMS_DIR, stream_name, filename)
            
            # Validate path is within STREAMS_DIR
            if not validate_path_within_streams_dir(video_path):
                errors.append(f'Access denied: {stream_name}/{filename}')
                continue
            
            if not os.path.exists(video_path):
                errors.append(f'Video file not found: {stream_name}/{filename}')
                continue
            
            try:
                # Delete video file
                os.remove(video_path)
                logger.info(f"Deleted recording: {video_path}")
                
                # Delete thumbnail if exists
                base_name = filename.rsplit('.', 1)[0]
                thumbnail_path = os.path.join(STREAMS_DIR, stream_name, base_name + '_thumb.jpg')
                if validate_path_within_streams_dir(thumbnail_path) and os.path.exists(thumbnail_path):
                    os.remove(thumbnail_path)
                    logger.info(f"Deleted thumbnail: {thumbnail_path}")

                # Delete KLV sidecar files
                delete_sidecar_files(stream_name, base_name)
                
                deleted_count += 1
            except Exception as e:
                errors.append(f'Failed to delete {stream_name}/{filename}: {str(e)}')
                logger.error(f"Error deleting recording {stream_name}/{filename}: {e}")
        
        return jsonify({
            'success': True,
            'deletedCount': deleted_count,
            'totalRequested': len(recordings),
            'errors': errors if errors else None
        })
        
    except Exception as e:
        logger.error(f"Error in bulk delete: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings/<path:stream_name>/<filename>/metadata', methods=['GET'])
def get_recording_metadata(stream_name, filename):
    """Get metadata for a specific recording"""
    try:
        file_path = os.path.join(STREAMS_DIR, stream_name, filename)
        
        if not os.path.exists(file_path):
            return jsonify({'error': 'Recording not found'}), 404
        
        metadata = analyze_recording(file_path)
        
        # Extract keywords from video file metadata tags
        # Keywords are stored in comment field as "Recording... | Keywords: xxx"
        keywords = ''
        if 'tags' in metadata and isinstance(metadata['tags'], dict):
            comment = metadata['tags'].get('comment', '')
            if comment:
                # Check if keywords are embedded in comment with delimiter
                if ' | Keywords: ' in comment:
                    keywords = comment.split(' | Keywords: ', 1)[1]
                elif comment.startswith('Keywords: '):
                    keywords = comment.replace('Keywords: ', '', 1)
                # Fallback: if comment doesn't look like recording metadata, use it as keywords
                elif not comment.startswith('Recording started'):
                    keywords = comment
        
        metadata['keywords'] = keywords
        
        return jsonify({'metadata': metadata})
    except Exception as e:
        logger.error(f"Error getting recording metadata: {e}")
        return jsonify({'error': str(e)}), 500


@recordings_bp.route('/api/recordings/<path:stream_name>/<filename>/keywords', methods=['POST'])
def update_recording_keywords(stream_name, filename):
    """Update keywords for a specific recording by embedding them into video metadata"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400
            
        keywords = data.get('keywords', '')
        
        # Validate keywords - remove pipe character as it's used as delimiter
        if keywords and '|' in keywords:
            keywords = keywords.replace('|', '')
        
        file_path = os.path.join(STREAMS_DIR, stream_name, filename)
        logger.info(f"Attempting to update keywords for: {file_path}")
        
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return jsonify({'error': 'Recording not found'}), 404
        
        # Create temporary file with proper extension so ffmpeg can detect format
        # Use .tmp.mov instead of just .tmp
        base_path, ext = os.path.splitext(file_path)
        temp_path = f"{base_path}.tmp{ext}"
        
        try:
            logger.info(f"Running ffmpeg to embed keywords: '{keywords}'")
            
            # First, read existing metadata to preserve it
            probe_cmd = [
                'ffprobe', '-v', 'error',
                '-show_entries', 'format_tags',
                '-of', 'json',
                file_path
            ]
            probe_result = subprocess.run(probe_cmd, capture_output=True, text=True)
            existing_tags = {}
            if probe_result.returncode == 0:
                probe_data = json.loads(probe_result.stdout)
                if 'format' in probe_data and 'tags' in probe_data['format']:
                    existing_tags = probe_data['format']['tags']
            
            # Build comment field: preserve recording metadata, add keywords with delimiter
            comment_parts = []
            existing_comment = existing_tags.get('comment', '')
            
            # Keep recording metadata if it exists, but strip out old keywords
            if existing_comment and existing_comment.startswith('Recording started'):
                # Remove any existing keywords section
                if ' | Keywords: ' in existing_comment:
                    existing_comment = existing_comment.split(' | Keywords: ')[0]
                comment_parts.append(existing_comment)
            
            # Sanitize and add keywords with delimiter
            if keywords:
                safe_keywords = sanitize_metadata(keywords)
                comment_parts.append(f'Keywords: {safe_keywords}')
            
            final_comment = ' | '.join(comment_parts) if comment_parts else sanitize_metadata(keywords)
            
            # Use ffmpeg to copy the video and update metadata without re-encoding
            # -map 0 copies all streams (video, audio, data/KLV)
            # -c copy means stream copy (no re-encoding, preserves KLV)
            cmd = [
                'ffmpeg',
                '-i', file_path,
                '-map', '0',           # Copy all streams including KLV data
                '-c', 'copy',          # Stream copy - no re-encoding
                '-metadata', f'comment={final_comment}'
            ]
            
            # Preserve other important existing metadata fields
            # If creation_time is missing, try to extract from comment
            if 'creation_time' not in existing_tags and 'comment' in existing_tags:
                comment_value = existing_tags['comment']
                if 'Recording started' in comment_value:
                    match = re.search(r'Recording started (\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)', comment_value)
                    if match:
                        existing_tags['creation_time'] = match.group(1)
            
            for field in ['title', 'artist', 'date', 'encoder', 'creation_time', 'timecode']:
                if field in existing_tags:
                    cmd.extend(['-metadata', f'{field}={existing_tags[field]}'])
            
            cmd.extend([
                '-y',                  # Overwrite output file
                temp_path
            ])
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                logger.error(f"ffmpeg error updating keywords: {result.stderr}")
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                return jsonify({'error': f'Failed to update video metadata: {result.stderr}'}), 500
            
            # Verify temp file was created
            if not os.path.exists(temp_path):
                logger.error(f"Temp file was not created: {temp_path}")
                return jsonify({'error': 'Failed to create temporary file'}), 500
            
            # Replace original file with updated one
            logger.info(f"Replacing original file with updated version")
            shutil.move(temp_path, file_path)
            
            logger.info(f"Successfully embedded keywords into {stream_name}/{filename}")
            return jsonify({'success': True, 'message': 'Keywords updated successfully'})
            
        except subprocess.TimeoutExpired:
            logger.error(f"ffmpeg timeout updating keywords for {filename}")
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'error': 'Operation timed out'}), 500
        except Exception as e:
            logger.error(f"Error embedding keywords: {e}", exc_info=True)
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass
            return jsonify({'error': f'Failed to embed keywords: {str(e)}'}), 500
            
    except Exception as e:
        logger.error(f"Error updating recording keywords: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500
