"""
This material is based upon work supported by the United States Air Force under contract number FA8750-24-S-B079 (Prime Contractor Smart Information Flow Technologies (SIFT)).  Any opinions, findings and conclusions or recommendations expressed in this material are those of the author(s) and do not necessarily reflect the views of the United States Air Force.
 Copyright (c) 2026 RTX BBN Technologies. Licensed to US Government with unlimited rights.

This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
This is distributed in the hope that it will be useful, but without any warranty, without even the implied warranty of merchantability or fitness for a particular purpose.  See the GNU General Public License for more details. https://www.gnu.org/licenses/

MediaMTX API client service
"""
import logging
import requests
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


class MediaMTXClient:
    """Client for MediaMTX API interactions"""
    
    def __init__(self, api_url: str):
        self.api_url = api_url
    
    def list_paths(self, timeout: int = 5) -> Optional[Dict]:
        """List all active paths from MediaMTX"""
        try:
            response = requests.get(f'{self.api_url}/v3/paths/list/', timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error listing paths: {e}")
            return None
    
    def get_path(self, path_name: str, timeout: int = 5) -> Optional[Dict]:
        """Get specific path details. Returns None if path not found (404)."""
        try:
            response = requests.get(f'{self.api_url}/v3/paths/get/{path_name}', timeout=timeout)
            if response.status_code == 404:
                return None  # Path has no active publisher — not an error
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as e:
            logger.warning(f"Error getting path {path_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error getting path {path_name}: {e}")
            return None
    
    def add_path(self, path_name: str, config: dict = None, timeout: int = 5) -> bool:
        """Add (or update) a persistent path configuration in MediaMTX.

        Tries POST /add/ first; if the path already exists (4xx), falls back
        to PATCH /patch/ so the call acts as an upsert.
        """
        try:
            payload = config or {'source': 'publisher'}
            response = requests.post(
                f'{self.api_url}/v3/config/paths/add/{path_name}',
                json=payload,
                timeout=timeout
            )
            if response.status_code in [200, 204]:
                logger.info(f"Added persistent path config: {path_name}")
                return True

            # Path likely already exists — try patch (upsert)
            logger.debug(
                f"add_path POST failed for {path_name} ({response.status_code}), trying PATCH"
            )
            patch_resp = requests.patch(
                f'{self.api_url}/v3/config/paths/patch/{path_name}',
                json=payload,
                timeout=timeout
            )
            if patch_resp.status_code in [200, 204]:
                logger.info(f"Patched existing path config: {path_name}")
                return True

            logger.warning(
                f"Failed to add/patch path {path_name}: "
                f"add={response.status_code}, patch={patch_resp.status_code} {patch_resp.text}"
            )
            return False
        except Exception as e:
            logger.error(f"Error adding path {path_name}: {e}")
            return False

    def delete_path(self, path_name: str, timeout: int = 5) -> bool:
        """Delete a path configuration"""
        try:
            response = requests.delete(
                f'{self.api_url}/v3/config/paths/delete/{path_name}',
                timeout=timeout
            )
            return response.status_code in [200, 204]
        except Exception as e:
            logger.error(f"Error deleting path {path_name}: {e}")
            return False

    def patch_global_config(self, payload: dict, timeout: int = 5) -> bool:
        """Patch MediaMTX global configuration at runtime."""
        try:
            response = requests.patch(
                f'{self.api_url}/v3/config/global/patch',
                json=payload,
                timeout=timeout
            )
            return response.status_code in [200, 204]
        except Exception as e:
            logger.error(f"Error patching global config: {e}")
            return False
    
    # MediaMTX v1.16 has per-protocol endpoints, no generic /connections/
    _CONN_ENDPOINTS = [
        ('srtconns',      'srtConn'),
        ('rtspsessions',  'rtspSession'),
        ('rtmpconns',     'rtmpConn'),
    ]

    def kick_connection(self, conn_type: str, connection_id: str, timeout: int = 5) -> bool:
        """Kick a specific connection by its protocol-specific endpoint.
        
        conn_type is the endpoint prefix, e.g. 'srtconns', 'rtspsessions'.
        """
        try:
            response = requests.post(
                f'{self.api_url}/v3/{conn_type}/kick/{connection_id}/',
                timeout=timeout
            )
            return response.status_code in [200, 204]
        except Exception as e:
            logger.error(f"Error kicking {conn_type} connection {connection_id}: {e}")
            return False

    def list_connections(self, timeout: int = 5) -> Optional[List]:
        """List all active connections across all protocol types.
        
        Returns a flat list of dicts, each with an extra '_conn_type' key
        indicating the kick endpoint prefix (e.g. 'srtconns').
        """
        all_connections = []
        for endpoint, _label in self._CONN_ENDPOINTS:
            try:
                response = requests.get(
                    f'{self.api_url}/v3/{endpoint}/list/', timeout=timeout
                )
                if response.status_code == 200:
                    data = response.json()
                    items = data.get('items', []) if isinstance(data, dict) else data
                    for item in items:
                        item['_conn_type'] = endpoint
                    all_connections.extend(items)
            except Exception as e:
                logger.debug(f"Could not list {endpoint}: {e}")
        return all_connections if all_connections else None
