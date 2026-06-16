"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

Settings API Blueprint - Configuration management endpoints
"""
from flask import Blueprint, request, jsonify
import os
import json
import platform
import shutil
import logging
import subprocess
from datetime import datetime, timezone
from werkzeug.utils import secure_filename

from app.config import (
    STREAMS_DIR, DATA_DIR, CERTS_DIR, EXTERNAL_CERTS_DIR, ACTIVE_CERTS_DIR,
    KLV_AVAILABLE, SRT_BUFFER_AVAILABLE,
    PULL_STREAM_BUFFER_SIZE, PULL_STREAM_MAX_DELAY,
    SERVER_SETTINGS, SERVER_SETTINGS_SCHEMA
)
from app.state import (
    active_recordings, active_pull_streams, pull_stream_configs,
    get_srt_buffer_manager
)
import app.state as app_state
from app.websocket.broadcast import broadcast

logger = logging.getLogger(__name__)

settings_bp = Blueprint('settings', __name__)

# Global settings that can be modified at runtime
server_settings = SERVER_SETTINGS.copy()


def _server_settings_file():
    """Path to the persisted server settings JSON file."""
    return os.path.join(DATA_DIR, 'server_settings.json')


def _save_server_settings():
    """Persist current server_settings to disk."""
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(_server_settings_file(), 'w') as f:
            json.dump(server_settings, f, indent=2)
    except Exception as e:
        logger.error(f"Error saving server settings: {e}")


def _apply_mtu_to_mediamtx(mtu: int):
    """Push udpMaxPayloadSize to the running MediaMTX instance."""
    from app.config import MEDIAMTX_API_URL
    from app.services.mediamtx import MediaMTXClient
    try:
        client = MediaMTXClient(MEDIAMTX_API_URL)
        success = client.patch_global_config({'udpMaxPayloadSize': mtu})
        if success:
            logger.info(f"Applied udpMaxPayloadSize={mtu} to MediaMTX")
        else:
            logger.warning(f"Failed to apply udpMaxPayloadSize={mtu} to MediaMTX")
    except Exception as e:
        logger.error(f"Error applying MTU to MediaMTX: {e}")


def load_and_apply_server_settings():
    """Load persisted settings from disk and apply relevant ones (e.g. MTU) to MediaMTX."""
    global server_settings
    settings_file = _server_settings_file()
    if os.path.exists(settings_file):
        try:
            with open(settings_file, 'r') as f:
                saved = json.load(f)
            for key, value in saved.items():
                if key in server_settings:
                    server_settings[key] = value
            logger.info("Loaded persisted server settings from disk")
        except Exception as e:
            logger.error(f"Error loading server settings: {e}")

    _apply_mtu_to_mediamtx(server_settings.get('udp_max_payload_size', 1452))


def get_auto_record_enabled():
    """Get auto_record_enabled from centralized state"""
    return app_state.auto_record_enabled


def set_auto_record_enabled(value: bool):
    """Set auto_record_enabled in centralized state"""
    app_state.auto_record_enabled = bool(value)


def _validate_setting(key: str, value):
    """
    Validate a setting value against the schema.
    Returns (coerced_value, error_message). error_message is None on success.
    """
    schema = SERVER_SETTINGS_SCHEMA.get(key)
    if not schema:
        return value, None  # unknown key — accept as-is (will be filtered by key check)

    expected_type = schema[0]

    # Type coercion / check
    if expected_type is bool:
        if not isinstance(value, bool):
            return None, f"Setting '{key}' must be a boolean"
    elif expected_type is int:
        if isinstance(value, bool):
            return None, f"Setting '{key}' must be an integer, not boolean"
        try:
            value = int(value)
        except (ValueError, TypeError):
            return None, f"Setting '{key}' must be an integer"
        if len(schema) == 3:
            min_val, max_val = schema[1], schema[2]
            if value < min_val or value > max_val:
                return None, f"Setting '{key}' must be between {min_val} and {max_val}"
    elif expected_type is str:
        if not isinstance(value, str):
            return None, f"Setting '{key}' must be a string"

    return value, None


def _srt_settings_file():
    """Path to the SRT settings JSON file."""
    return os.path.join(DATA_DIR, 'srt_settings.json')


def _abr_settings_file():
    """Path to the ABR settings JSON file."""
    return os.path.join(DATA_DIR, 'abr_settings.json')


# Preset options for ABR dropdowns
ABR_RESOLUTION_OPTIONS = [
    {'label': '1080p (1920x1080)', 'value': '1920x1080'},
    {'label': '720p (1280x720)',   'value': '1280x720'},
    {'label': '540p (960x540)',    'value': '960x540'},
    {'label': '480p (854x480)',    'value': '854x480'},
    {'label': '360p (640x360)',    'value': '640x360'},
    {'label': '240p (426x240)',    'value': '426x240'},
]

ABR_BITRATE_OPTIONS = [
    {'label': '5 Mbps',   'value': 5000},
    {'label': '4 Mbps',   'value': 4000},
    {'label': '3 Mbps',   'value': 3000},
    {'label': '2.5 Mbps', 'value': 2500},
    {'label': '2 Mbps',   'value': 2000},
    {'label': '1.5 Mbps', 'value': 1500},
    {'label': '1.2 Mbps', 'value': 1200},
    {'label': '1 Mbps',   'value': 1000},
    {'label': '800 Kbps', 'value': 800},
    {'label': '600 Kbps', 'value': 600},
    {'label': '400 Kbps', 'value': 400},
    {'label': '300 Kbps', 'value': 300},
]

ABR_AUDIO_BITRATE_OPTIONS = [
    {'label': '128 Kbps', 'value': 128},
    {'label': '96 Kbps',  'value': 96},
    {'label': '64 Kbps',  'value': 64},
    {'label': '48 Kbps',  'value': 48},
]

DEFAULT_ABR_SETTINGS = {
    'medium_enabled': False,
    'high': {
        'resolution': '1280x720',
        'bitrate': 2500,
        'audio_bitrate': 128,
    },
    'medium': {
        'resolution': '960x540',
        'bitrate': 1200,
        'audio_bitrate': 96,
    },
    'low': {
        'resolution': '640x360',
        'bitrate': 600,
        'audio_bitrate': 64,
    },
}


def _data_certs_dir():
    """Path to the data/certs directory for backup copies."""
    return os.path.join(os.path.dirname(STREAMS_DIR), 'data', 'certs')


@settings_bp.route('/api/status', methods=['GET'])
def get_status():
    """Get overall system status (compatibility endpoint for client.js)"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'activeStreams': len(active_recordings),
        'activeRecordings': len(active_recordings),
        'klvAvailable': KLV_AVAILABLE,
        'activePullStreams': len(active_pull_streams),
        'pullStreamRetryEnabled': server_settings.get('auto_reconnect', True),
        'pullStreamConfigs': len(pull_stream_configs),
        'autoRecordEnabled': get_auto_record_enabled()
    })


@settings_bp.route('/api/settings', methods=['GET'])
def get_settings():
    """Get all server settings"""
    # Add disk space info
    try:
        disk_usage = shutil.disk_usage(STREAMS_DIR)

        if platform.system() == 'Windows':
            device = os.path.splitdrive(os.path.abspath(STREAMS_DIR))[0] or 'C:'
        else:
            try:
                abs_path = os.path.abspath(STREAMS_DIR)
                while not os.path.ismount(abs_path):
                    abs_path = os.path.dirname(abs_path)
                device = abs_path
            except Exception:
                device = '/'

        disk_info = {
            'total_gb': round(disk_usage.total / (1024**3), 2),
            'used_gb': round(disk_usage.used / (1024**3), 2),
            'free_gb': round(disk_usage.free / (1024**3), 2),
            'percent_used': round((disk_usage.used / disk_usage.total) * 100, 1),
            'device': device,
            'path': STREAMS_DIR
        }
    except Exception as e:
        logger.error(f"Error getting disk usage: {e}")
        disk_info = {}

    return jsonify({
        'settings': server_settings,
        'disk': disk_info,
        'autoRecord': get_auto_record_enabled(),
        'pullStreamBufferSize': PULL_STREAM_BUFFER_SIZE,
        'pullStreamMaxDelay': PULL_STREAM_MAX_DELAY
    })


@settings_bp.route('/api/settings', methods=['POST'])
def update_settings():
    """Update server settings with type validation"""
    global server_settings
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body required'}), 400

        updated = {}
        errors = []

        for key, value in data.items():
            if key not in server_settings:
                continue

            coerced, error = _validate_setting(key, value)
            if error:
                errors.append(error)
                continue

            old_value = server_settings[key]
            server_settings[key] = coerced
            updated[key] = {'old': old_value, 'new': coerced}
            logger.info(f"Setting updated: {key} = {coerced} (was {old_value})")

        if errors and not updated:
            return jsonify({'error': '; '.join(errors)}), 400

        _save_server_settings()

        if 'udp_max_payload_size' in updated:
            _apply_mtu_to_mediamtx(server_settings['udp_max_payload_size'])

        broadcast('settings_updated', {'settings': server_settings})

        result = {
            'success': True,
            'message': f'Updated {len(updated)} setting(s)',
            'updated': updated,
            'settings': server_settings
        }
        if errors:
            result['warnings'] = errors

        return jsonify(result)
    except Exception as e:
        logger.error(f"Error updating settings: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/auto-record-status', methods=['GET'])
def get_auto_record_status():
    """Get auto-record status (compatibility endpoint for settings page)"""
    return jsonify({'enabled': get_auto_record_enabled()})


@settings_bp.route('/api/auto-record-toggle', methods=['POST'])
def toggle_auto_record():
    """Toggle auto-record (compatibility endpoint for settings page)"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body required'}), 400

        enabled = data.get('enabled', False)
        set_auto_record_enabled(enabled)

        logger.info(f"Auto-record toggled to: {get_auto_record_enabled()}")
        broadcast('auto_record_changed', {'enabled': get_auto_record_enabled()})

        return jsonify({
            'success': True,
            'enabled': get_auto_record_enabled(),
            'message': f"Auto-record {'enabled' if get_auto_record_enabled() else 'disabled'}"
        })
    except Exception as e:
        logger.error(f"Error toggling auto-record: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/auto-record', methods=['GET'])
def get_auto_record_setting():
    """Get auto-record setting"""
    return jsonify({'enabled': get_auto_record_enabled()})


@settings_bp.route('/api/settings/auto-record', methods=['POST'])
def set_auto_record_setting():
    """Enable or disable auto-record for new inbound streams"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body required'}), 400

        enabled = data.get('enabled', False)
        set_auto_record_enabled(enabled)

        logger.info(f"Auto-record setting changed to: {get_auto_record_enabled()}")
        broadcast('auto_record_changed', {'enabled': get_auto_record_enabled()})

        return jsonify({
            'success': True,
            'enabled': get_auto_record_enabled(),
            'message': f"Auto-record {'enabled' if get_auto_record_enabled() else 'disabled'}"
        })
    except Exception as e:
        logger.error(f"Error setting auto-record: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/srt', methods=['GET'])
def get_srt_settings():
    """Get SRT-specific settings from settings file"""
    try:
        settings_file = _srt_settings_file()

        default_settings = {
            'enabled': True,
            'port': 8890,
            'latency': 120,
            'maxbw': 0,
            'pbkeylen': 0,
            'passphrase': '',
            'transtype': 'live',
            'tlpktdrop': True,
            'nakreport': True,
            'conntimeo': 3000,
            'peeridletimeo': 5000
        }

        if os.path.exists(settings_file):
            with open(settings_file, 'r') as f:
                saved_settings = json.load(f)
                default_settings.update(saved_settings)

        return jsonify({'success': True, 'settings': default_settings})
    except Exception as e:
        logger.error(f"Error reading SRT settings: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/srt', methods=['POST'])
def update_srt_settings():
    """Update SRT-specific settings - stored in JSON file for URL parameter generation"""
    try:
        settings_file = _srt_settings_file()
        data = request.get_json()

        # Validate input
        if 'latency' in data:
            latency = int(data['latency'])
            if latency < 20 or latency > 8000:
                return jsonify({'error': 'SRT latency must be between 20 and 8000 ms'}), 400

        if 'pbkeylen' in data:
            pbkeylen = int(data['pbkeylen'])
            if pbkeylen not in [0, 16, 24, 32]:
                return jsonify({'error': 'Encryption key length must be 0, 16, 24, or 32 bytes'}), 400

        if 'passphrase' in data and data.get('pbkeylen', 0) > 0:
            passphrase = str(data['passphrase'])
            if len(passphrase) < 10 or len(passphrase) > 79:
                return jsonify({'error': 'SRT passphrase must be between 10 and 79 characters'}), 400

        # Ensure data directory exists
        os.makedirs(DATA_DIR, exist_ok=True)

        # Load current settings if file exists
        current_settings = {}
        if os.path.exists(settings_file):
            with open(settings_file, 'r') as f:
                current_settings = json.load(f)

        # Update settings
        updated = {}
        for key, value in data.items():
            old_value = current_settings.get(key)
            current_settings[key] = value
            updated[key] = {'old': old_value, 'new': value}
            logger.info(f"SRT setting updated: {key} = {value} (was {old_value})")

        # Save updated settings
        with open(settings_file, 'w') as f:
            json.dump(current_settings, f, indent=2)

        # Generate example URL with these settings
        example_params = []
        if current_settings.get('latency'):
            example_params.append(f"latency={current_settings['latency']}")
        if current_settings.get('maxbw'):
            example_params.append(f"maxbw={current_settings['maxbw']*1000000}")
        if current_settings.get('pbkeylen') and current_settings.get('pbkeylen') > 0:
            example_params.append(f"pbkeylen={current_settings['pbkeylen']}")
            example_params.append(f"passphrase={current_settings.get('passphrase', '')}")
        if current_settings.get('transtype'):
            example_params.append(f"transtype={current_settings['transtype']}")

        param_string = '&'.join(example_params)
        example_url = f"srt://SERVER_IP:8890?streamid=publish:STREAM_NAME&{param_string}"

        return jsonify({
            'success': True,
            'message': f'Updated {len(updated)} SRT setting(s). Use these parameters in your SRT URL.',
            'updated': updated,
            'settings': current_settings,
            'example_url': example_url,
            'note': 'SRT settings are applied via URL parameters, not server config. Copy the example URL pattern for your streaming client.'
        })

    except Exception as e:
        logger.error(f"Error updating SRT settings: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/abr', methods=['GET'])
def get_abr_settings():
    """Get ABR rendition settings and dropdown options."""
    try:
        settings_file = _abr_settings_file()
        settings = dict(DEFAULT_ABR_SETTINGS)
        # Deep copy nested dicts
        for tier in ('high', 'medium', 'low'):
            settings[tier] = dict(DEFAULT_ABR_SETTINGS[tier])

        if os.path.exists(settings_file):
            with open(settings_file, 'r') as f:
                saved = json.load(f)
                settings['medium_enabled'] = saved.get('medium_enabled', False)
                for tier in ('high', 'medium', 'low'):
                    if tier in saved:
                        settings[tier].update(saved[tier])

        return jsonify({
            'success': True,
            'settings': settings,
            'options': {
                'resolutions': ABR_RESOLUTION_OPTIONS,
                'bitrates': ABR_BITRATE_OPTIONS,
                'audio_bitrates': ABR_AUDIO_BITRATE_OPTIONS,
            }
        })
    except Exception as e:
        logger.error(f"Error reading ABR settings: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/abr', methods=['POST'])
def update_abr_settings():
    """Update ABR rendition settings and apply to running ABR processes."""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'Request body required'}), 400

        settings_file = _abr_settings_file()

        # Load current
        current = dict(DEFAULT_ABR_SETTINGS)
        for tier in ('high', 'medium', 'low'):
            current[tier] = dict(DEFAULT_ABR_SETTINGS[tier])
        if os.path.exists(settings_file):
            with open(settings_file, 'r') as f:
                saved = json.load(f)
                current['medium_enabled'] = saved.get('medium_enabled', False)
                for tier in ('high', 'medium', 'low'):
                    if tier in saved:
                        current[tier].update(saved[tier])

        # Valid values for validation
        valid_resolutions = {opt['value'] for opt in ABR_RESOLUTION_OPTIONS}
        valid_bitrates = {opt['value'] for opt in ABR_BITRATE_OPTIONS}
        valid_audio_bitrates = {opt['value'] for opt in ABR_AUDIO_BITRATE_OPTIONS}

        # Update from request
        if 'medium_enabled' in data:
            current['medium_enabled'] = bool(data['medium_enabled'])

        errors = []
        for tier in ('high', 'medium', 'low'):
            if tier in data and isinstance(data[tier], dict):
                tier_data = data[tier]
                if 'resolution' in tier_data:
                    if tier_data['resolution'] not in valid_resolutions:
                        errors.append(f"{tier}: invalid resolution '{tier_data['resolution']}'")
                    else:
                        current[tier]['resolution'] = tier_data['resolution']
                if 'bitrate' in tier_data:
                    bv = int(tier_data['bitrate'])
                    if bv not in valid_bitrates:
                        errors.append(f"{tier}: invalid bitrate {bv}")
                    else:
                        current[tier]['bitrate'] = bv
                if 'audio_bitrate' in tier_data:
                    ab = int(tier_data['audio_bitrate'])
                    if ab not in valid_audio_bitrates:
                        errors.append(f"{tier}: invalid audio bitrate {ab}")
                    else:
                        current[tier]['audio_bitrate'] = ab

        if errors:
            return jsonify({'error': '; '.join(errors)}), 400

        # Persist
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(settings_file, 'w') as f:
            json.dump(current, f, indent=2)
        logger.info(f"ABR settings updated: {current}")

        # Apply to running ABR processes by rebuilding renditions and restarting
        try:
            from app.services.abr import abr_manager
            abr_manager.apply_settings(current)
        except Exception as apply_err:
            logger.warning(f"Could not apply ABR settings to running processes: {apply_err}")

        broadcast('abr_settings_updated', {'settings': current})

        return jsonify({
            'success': True,
            'message': 'ABR settings saved. Running ABR transcodes will restart with new settings.',
            'settings': current
        })
    except Exception as e:
        logger.error(f"Error updating ABR settings: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/streams/<stream_name>/buffer', methods=['POST'])
def enable_stream_buffer(stream_name):
    """Enable SRT buffering and auto-reconnect for a specific stream"""
    try:
        if not SRT_BUFFER_AVAILABLE:
            return jsonify({'error': 'SRT buffer module not available'}), 503

        data = request.get_json() or {}
        enable = data.get('enable', True)

        srt_manager = get_srt_buffer_manager()
        if not srt_manager:
            return jsonify({'error': 'SRT buffer manager not available'}), 503

        if enable:
            added = srt_manager.add_stream(stream_name)
            if added:
                logger.info(f"Enabled SRT buffering for stream: {stream_name}")
            return jsonify({
                'success': True,
                'message': f'SRT buffering {"enabled" if added else "already enabled"} for {stream_name}',
                'stream': stream_name,
                'buffering': True
            })
        else:
            removed = srt_manager.remove_stream(stream_name)
            if removed:
                logger.info(f"Disabled SRT buffering for stream: {stream_name}")
            return jsonify({
                'success': True,
                'message': f'SRT buffering {"disabled" if removed else "was not enabled"} for {stream_name}',
                'stream': stream_name,
                'buffering': False
            })
    except Exception as e:
        logger.error(f"Error managing SRT buffer for {stream_name}: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/certificates/status', methods=['GET'])
def get_certificate_status():
    """Get TLS certificate status"""
    try:
        external_cert = os.path.join(EXTERNAL_CERTS_DIR, 'server.crt')
        external_key = os.path.join(EXTERNAL_CERTS_DIR, 'server.key')
        custom_certs_available = os.path.exists(external_cert) and os.path.exists(external_key)

        active_cert = os.path.join(ACTIVE_CERTS_DIR, 'server.crt')
        active_key = os.path.join(ACTIVE_CERTS_DIR, 'server.key')
        cert_exists = os.path.exists(active_cert)
        key_exists = os.path.exists(active_key)

        status = {
            'installed': custom_certs_available,
            'cert_exists': cert_exists,
            'key_exists': key_exists,
            'cert_file': 'server.crt' if cert_exists else None,
            'key_file': 'server.key' if key_exists else None,
            'custom_certs': custom_certs_available
        }

        if cert_exists:
            try:
                result = subprocess.run(
                    ['openssl', 'x509', '-in', active_cert, '-noout', '-subject', '-issuer', '-enddate'],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                if result.returncode == 0:
                    for line in result.stdout.strip().split('\n'):
                        if line.startswith('subject='):
                            status['subject'] = line.replace('subject=', '').strip()
                        elif line.startswith('issuer='):
                            status['issuer'] = line.replace('issuer=', '').strip()
                        elif line.startswith('notAfter='):
                            status['expires'] = line.replace('notAfter=', '').strip()
            except Exception as e:
                logger.warning(f"Could not read certificate details: {e}")

        return jsonify(status)

    except Exception as e:
        logger.error(f"Error getting certificate status: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/certificates/upload', methods=['POST'])
def upload_certificates():
    """Upload custom TLS certificates"""
    try:
        if 'certificate' not in request.files or 'key' not in request.files:
            return jsonify({'error': 'Both certificate and key files are required'}), 400

        cert_file = request.files['certificate']
        key_file = request.files['key']

        if cert_file.filename == '' or key_file.filename == '':
            return jsonify({'error': 'No files selected'}), 400

        cert_filename = secure_filename(cert_file.filename)
        key_filename = secure_filename(key_file.filename)

        allowed_cert_ext = ['.crt', '.pem', '.cer']
        allowed_key_ext = ['.key', '.pem']

        cert_ext = os.path.splitext(cert_filename)[1].lower()
        key_ext = os.path.splitext(key_filename)[1].lower()

        if cert_ext not in allowed_cert_ext:
            return jsonify({'error': f'Certificate must be one of: {", ".join(allowed_cert_ext)}'}), 400
        if key_ext not in allowed_key_ext:
            return jsonify({'error': f'Key must be one of: {", ".join(allowed_key_ext)}'}), 400

        os.makedirs(CERTS_DIR, exist_ok=True)

        cert_path = os.path.join(CERTS_DIR, 'server.crt')
        key_path = os.path.join(CERTS_DIR, 'server.key')

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        if os.path.exists(cert_path):
            shutil.move(cert_path, f'{cert_path}.backup.{timestamp}')
        if os.path.exists(key_path):
            shutil.move(key_path, f'{key_path}.backup.{timestamp}')

        cert_file.save(cert_path)
        key_file.save(key_path)

        os.chmod(cert_path, 0o644)
        os.chmod(key_path, 0o600)

        # Validate certificate
        try:
            result = subprocess.run(
                ['openssl', 'x509', '-in', cert_path, '-noout'],
                capture_output=True,
                timeout=5
            )
            if result.returncode != 0:
                if os.path.exists(f'{cert_path}.backup.{timestamp}'):
                    shutil.move(f'{cert_path}.backup.{timestamp}', cert_path)
                if os.path.exists(f'{key_path}.backup.{timestamp}'):
                    shutil.move(f'{key_path}.backup.{timestamp}', key_path)
                return jsonify({'error': 'Invalid certificate file'}), 400
        except Exception as e:
            logger.warning(f"Could not validate certificate: {e}")

        logger.info(f"TLS certificates uploaded successfully to {CERTS_DIR}")

        # Also copy to data/certs if writable
        try:
            data_certs = _data_certs_dir()
            if os.path.exists(os.path.dirname(data_certs)):
                os.makedirs(data_certs, exist_ok=True)
                shutil.copy2(cert_path, os.path.join(data_certs, 'server.crt'))
                shutil.copy2(key_path, os.path.join(data_certs, 'server.key'))
                os.chmod(os.path.join(data_certs, 'server.key'), 0o600)
                logger.info(f"Certificates also copied to {data_certs}")
        except Exception as copy_error:
            logger.warning(f"Could not copy to data/certs (non-fatal): {copy_error}")

        return jsonify({
            'success': True,
            'message': 'Certificates uploaded successfully. Restart container to apply changes: docker-compose restart',
            'cert_file': 'server.crt',
            'key_file': 'server.key',
            'location': CERTS_DIR
        })

    except Exception as e:
        logger.error(f"Error uploading certificates: {e}")
        return jsonify({'error': str(e)}), 500


@settings_bp.route('/api/settings/certificates/generate', methods=['POST'])
def generate_self_signed():
    """Generate a new self-signed certificate"""
    try:
        os.makedirs(CERTS_DIR, exist_ok=True)

        cert_path = os.path.join(CERTS_DIR, 'server.crt')
        key_path = os.path.join(CERTS_DIR, 'server.key')

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
        if os.path.exists(cert_path):
            shutil.move(cert_path, f'{cert_path}.backup.{timestamp}')
        if os.path.exists(key_path):
            shutil.move(key_path, f'{key_path}.backup.{timestamp}')

        result = subprocess.run([
            'openssl', 'req', '-x509',
            '-newkey', 'rsa:4096',
            '-keyout', key_path,
            '-out', cert_path,
            '-days', '3650',
            '-nodes',
            '-subj', '/CN=localhost'
        ], capture_output=True, text=True, timeout=30)

        if result.returncode != 0:
            if os.path.exists(f'{cert_path}.backup.{timestamp}'):
                shutil.move(f'{cert_path}.backup.{timestamp}', cert_path)
            if os.path.exists(f'{key_path}.backup.{timestamp}'):
                shutil.move(f'{key_path}.backup.{timestamp}', key_path)
            return jsonify({'error': f'Failed to generate certificate: {result.stderr}'}), 500

        os.chmod(cert_path, 0o644)
        os.chmod(key_path, 0o600)

        logger.info(f"Self-signed certificate generated successfully in {CERTS_DIR}")

        # Also copy to data/certs if writable
        try:
            data_certs = _data_certs_dir()
            if os.path.exists(os.path.dirname(data_certs)):
                os.makedirs(data_certs, exist_ok=True)
                shutil.copy2(cert_path, os.path.join(data_certs, 'server.crt'))
                shutil.copy2(key_path, os.path.join(data_certs, 'server.key'))
                os.chmod(os.path.join(data_certs, 'server.key'), 0o600)
                logger.info(f"Certificates also copied to {data_certs}")
        except Exception as copy_error:
            logger.warning(f"Could not copy to data/certs (non-fatal): {copy_error}")

        return jsonify({
            'success': True,
            'message': 'Self-signed certificate generated. Restart container to apply changes: docker-compose restart',
            'location': CERTS_DIR
        })

    except subprocess.TimeoutExpired:
        return jsonify({'error': 'Certificate generation timed out'}), 500
    except Exception as e:
        logger.error(f"Error generating self-signed certificate: {e}")
        return jsonify({'error': str(e)}), 500
