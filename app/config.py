"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

Application configuration and settings
"""
import os
import sys
import secrets

# Flask Configuration
PORT = int(os.environ.get('PORT', 3000))

# Generate secure secret key if not provided
_env_secret = os.environ.get('SECRET_KEY', '')
if _env_secret and _env_secret != 'dev-secret-key-change-in-production':
    SECRET_KEY = _env_secret
else:
    # Generate cryptographically secure random key
    SECRET_KEY = secrets.token_hex(32)
    if not os.environ.get('SECRET_KEY'):
        print("WARNING: No SECRET_KEY set in environment. Using generated random key (sessions won't persist across restarts).")

# MediaMTX Configuration
# Use 127.0.0.1 (not 'localhost') to avoid IPv6 resolution on Windows
MEDIAMTX_API_URL = os.environ.get('MEDIAMTX_API_URL', 'http://127.0.0.1:8889')
MEDIAMTX_RTSP_URL = os.environ.get('MEDIAMTX_RTSP_URL', 'rtsp://127.0.0.1:8554')

# Directory Configuration
STREAMS_DIR = os.environ.get('STREAMS_DIR', '/opt/app/streams')
# Places a copy of recordings in a shared volume.
SHARED_VIDEOS_DIR = os.environ.get('SHARED_VIDEOS_DIR', '/opt/app/shared_videos')
LOGS_DIR = os.environ.get('LOGS_DIR', '/opt/app/logs')
DATA_DIR = os.environ.get('DATA_DIR', '/opt/app/data')
CERTS_DIR = os.environ.get('CERTS_DIR', os.path.join(STREAMS_DIR, '.certs'))
EXTERNAL_CERTS_DIR = os.environ.get('EXTERNAL_CERTS_DIR', '/opt/app/external-certs')
ACTIVE_CERTS_DIR = os.environ.get('ACTIVE_CERTS_DIR', '/opt/app/certs')

# CORS Configuration
CORS_ORIGINS = os.environ.get('CORS_ORIGINS', '*')

# Logging Configuration
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB per file
LOG_BACKUP_COUNT = 5  # Keep 5 backup files

# Check for optional modules
# KLV Module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shared'))
try:
    import klv
    KLV_AVAILABLE = True
except ImportError:
    KLV_AVAILABLE = False
    print("Warning: KLV module not available")

# SRT Buffer Module
try:
    from srt_buffer import get_manager as get_srt_buffer_manager
    SRT_BUFFER_AVAILABLE = True
except ImportError:
    SRT_BUFFER_AVAILABLE = False
    print("Warning: SRT buffer module not available")

# Pull Stream Configuration
PULL_STREAM_BUFFER_SIZE = int(os.environ.get('PULL_STREAM_BUFFER_SIZE', 10485760))  # 10MB default buffer
PULL_STREAM_MAX_DELAY = int(os.environ.get('PULL_STREAM_MAX_DELAY', 1000000))  # 1 second in microseconds (lower = less latency)

# Server Settings - Recording & Stream Management
# Type schema for validation: maps each key to (type, min, max) or (type,) for no range check
# str keys use (str,) only
SERVER_SETTINGS_SCHEMA = {
    'segmented_recording': (bool,),
    'segment_duration': (int, 10, 86400),
    'max_file_size_gb': (int, 1, 1000),
    'auto_cleanup_enabled': (bool,),
    'cleanup_days': (int, 1, 3650),
    'min_free_space_gb': (int, 1, 1000),
    'auto_reconnect': (bool,),
    'reconnect_delay': (int, 1, 300),
    'max_reconnect_attempts': (int, -1, 10000),
    'exponential_backoff': (bool,),
    'max_backoff_delay': (int, 1, 3600),
    'health_check_enabled': (bool,),
    'stall_detection_enabled': (bool,),
    'stall_threshold_seconds': (int, 5, 600),
    'srt_buffer_enabled': (bool,),
    'srt_auto_reconnect': (bool,),
    'srt_reconnect_delay': (int, 1, 300),
    'srt_max_buffer_seconds': (int, 1, 300),
    'rtsp_transport': (str,),
    'connection_timeout': (int, 100000, 60000000),
    'enable_ffmpeg_reconnect': (bool,),
    'standby_enabled': (bool,),
    'standby_timeout_minutes': (int, 0, 14400),  # 0 = infinite, max 10 days
    'udp_max_payload_size': (int, 576, 65535),
}

SERVER_SETTINGS = {
    # Recording settings
    'segmented_recording': False,
    'segment_duration': 600,  # 10 minutes in seconds
    'max_file_size_gb': 10,
    'auto_cleanup_enabled': False,
    'cleanup_days': 30,
    'min_free_space_gb': 10,
    
    # Stream recovery settings
    'auto_reconnect': True,
    'reconnect_delay': 5,
    'max_reconnect_attempts': -1,  # -1 = unlimited
    'exponential_backoff': False,
    'max_backoff_delay': 60,
    
    # Health monitoring
    'health_check_enabled': True,
    'stall_detection_enabled': True,
    'stall_threshold_seconds': 30,
    
    # SRT buffering and recovery
    'srt_buffer_enabled': True,
    'srt_auto_reconnect': True,
    'srt_reconnect_delay': 2,
    'srt_max_buffer_seconds': 30,
    
    # Network resilience
    'rtsp_transport': 'tcp',  # tcp or udp
    'connection_timeout': 5000000,  # microseconds
    'enable_ffmpeg_reconnect': True,
    
    # Stream standby / persistence
    'standby_enabled': True,
    'standby_timeout_minutes': 60,  # 0 = never expire

    # Network / MTU
    'udp_max_payload_size': 1452,  # bytes; matches mediaMTX default
}
