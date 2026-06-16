"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

FFmpeg codec detection utilities
"""
import subprocess
import json
import logging
from typing import Optional, Dict

logger = logging.getLogger(__name__)


def detect_stream_codec(stream_url: str, timeout: int = 5) -> Optional[Dict[str, any]]:
    """
    Detect codec (H.264/H.265), resolution, and available streams from a stream using ffprobe
    Supports RTSP and SRT protocols
    
    Args:
        stream_url: Stream URL (rtsp://... or srt://...)
        timeout: Probe timeout in seconds
    
    Returns:
        dict: {codec: 'h264'|'hevc', width: int, height: int, has_audio: bool, has_data: bool, protocol: 'rtsp'|'srt'} or None on failure
    """
    logger.info(f"Detecting codec for stream: {stream_url}")
    
    try:
        # Build ffprobe command based on protocol
        cmd = [
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_streams'
        ]
        
        # Add protocol-specific options
        if stream_url.startswith('rtsp://') or stream_url.startswith('rtsps://'):
            cmd.extend(['-rtsp_transport', 'tcp'])
            protocol = 'rtsp'
        elif stream_url.startswith('srt://'):
            protocol = 'srt'
        else:
            protocol = 'unknown'
        
        cmd.append(stream_url)
        
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        
        if result.returncode != 0:
            logger.error(f"ffprobe failed: {result.stderr}")
            return None
        
        probe_data = json.loads(result.stdout)
        if not probe_data.get('streams'):
            logger.error("No streams found")
            return None
        
        # Analyze all streams
        video_codec = 'unknown'
        width = 0
        height = 0
        has_audio = False
        has_data = False
        
        for stream in probe_data.get('streams', []):
            codec_type = stream.get('codec_type', '')
            
            if codec_type == 'video' and video_codec == 'unknown':
                codec_name = stream.get('codec_name', 'unknown')
                if codec_name in ['h264', 'avc']:
                    video_codec = 'h264'
                elif codec_name in ['hevc', 'h265']:
                    video_codec = 'hevc'
                elif codec_name in ['av1', 'libaom-av1']:
                    video_codec = 'av1'
                else:
                    video_codec = codec_name
                width = stream.get('width', 0)
                height = stream.get('height', 0)
            elif codec_type == 'audio':
                has_audio = True
            elif codec_type == 'data':
                has_data = True
                logger.info(f"Detected data stream (likely KLV metadata): codec={stream.get('codec_name')}")
        
        logger.info(f"Detected: {video_codec} {width}x{height}, audio={has_audio}, data/KLV={has_data}, protocol={protocol}")
        
        return {
            'codec': video_codec,
            'width': width,
            'height': height,
            'has_audio': has_audio,
            'has_data': has_data,
            'protocol': protocol
        }
        
    except subprocess.TimeoutExpired:
        logger.error(f"ffprobe timeout after {timeout}s")
        return None
    except Exception as e:
        logger.error(f"Error detecting codec: {e}")
        return None


def analyze_recording(file_path: str) -> dict:
    """
    Analyze recording with ffprobe to extract codec, streams, timecode info
    """
    try:
        result = subprocess.run([
            'ffprobe',
            '-v', 'quiet',
            '-print_format', 'json',
            '-show_format',
            '-show_streams',
            file_path
        ], capture_output=True, text=True, timeout=10)
        
        if result.returncode != 0:
            return {'error': 'ffprobe failed', 'stderr': result.stderr}
        
        probe_data = json.loads(result.stdout)
        
        # Extract codec from video stream
        codec = 'unknown'
        for stream in probe_data.get('streams', []):
            if stream.get('codec_type') == 'video':
                codec = stream.get('codec_name', 'unknown')
                break
        
        # Check for tmcd track
        has_tmcd = any(s.get('codec_tag_string') == 'tmcd' for s in probe_data.get('streams', []))
        
        format_info = probe_data.get('format', {})
        
        return {
            'codec': codec,
            'streams': len(probe_data.get('streams', [])),
            'has_tmcd': has_tmcd,
            'duration': float(format_info.get('duration', 0)),
            'size': int(format_info.get('size', 0)),
            'format_name': format_info.get('format_name', ''),
            'tags': format_info.get('tags', {})
        }
        
    except Exception as e:
        logger.error(f"Error analyzing recording: {e}")
        return {'error': str(e)}
