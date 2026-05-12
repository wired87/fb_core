"""
Firebase Realtime Database Manager for fb_core.

Provides low-level operations against Firebase Realtime Database using the admin SDK.
"""

import os
import logging
from typing import Any, Callable, Optional, Dict

import firebase_admin
from firebase_admin import db

logger = logging.getLogger(__name__)


class FBRTDBMgr:
    """Manages operations against Firebase Realtime Database."""

    def __init__(self):
        """Initialize the database manager.
        
        Requires that firebase_admin.initialize_app() has already been called.
        """
        try:
            self.app = firebase_admin.get_app()
        except ValueError:
            logger.warning("Firebase app not initialized. Some operations will fail.")
            self.app = None

    def _is_available(self) -> bool:
        """Return True if the Firebase app is initialized and has a DB URL configured."""
        if not self.app:
            return False
        try:
            # db.reference() will raise ValueError if no databaseURL is set
            db.reference("/")
            return True
        except Exception:
            return False

    def get_data(self, path: str) -> Optional[Any]:
        """Retrieve data from the specified path in RTDB.
        
        Args:
            path: The database path (e.g., 'users/uid123/credits')
            
        Returns:
            The data at the path, or None if not found or error occurs.
        """
        try:
            if not self._is_available():
                return None
            ref = db.reference(path)
            data = ref.get()
            return data
        except Exception as e:
            logger.warning(f"Error reading {path}: {e}")
            return None

    def update_data(self, path: str, data: Dict[str, Any]) -> bool:
        """Update data at the specified path in RTDB.
        
        Args:
            path: The database path
            data: Dictionary of data to update
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self._is_available():
                return False
            ref = db.reference(path)
            ref.update(data)
            return True
        except Exception as e:
            logger.warning(f"Error updating {path}: {e}")
            return False

    def transact(
        self,
        path: str,
        update_fn: Callable[[Any], Any],
    ) -> bool:
        """Perform an atomic transaction at the specified path.
        
        Args:
            path: The database path
            update_fn: Function that takes current value and returns new value
            
        Returns:
            True if transaction was committed, False if aborted.
        """
        try:
            if not self._is_available():
                return False
            ref = db.reference(path)
            result = ref.transaction(update_fn)
            # Firebase SDK returns (committed, result_data) tuple
            # or just the committed boolean depending on SDK version
            if isinstance(result, tuple):
                return bool(result[0])
            return bool(result)
        except Exception as e:
            logger.warning(f"Error in transaction on {path}: {e}")
            return False

    def set_data(self, path: str, data: Any) -> bool:
        """Set data at the specified path (overwrites existing).
        
        Args:
            path: The database path
            data: Data to set
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self._is_available():
                return False
            ref = db.reference(path)
            if isinstance(data, dict):
                # update merges, safe against accidental overwrites
                ref.update(data)
            else:
                ref.set(data)
            return True
        except Exception as e:
            logger.warning(f"Error setting {path}: {e}")
            return False

    def remove_data(self, path: str) -> bool:
        """Delete data at the specified path.
        
        Args:
            path: The database path
            
        Returns:
            True if successful, False otherwise.
        """
        try:
            if not self._is_available():
                return False
            ref = db.reference(path)
            ref.delete()
            return True
        except Exception as e:
            logger.warning(f"Error removing {path}: {e}")
            return False
