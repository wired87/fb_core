"""
Firebase Core Admin for user and credit management.

This module provides the FirebaseAdmin class for managing user spaces, credit operations,
and history tracking in Firebase Realtime Database using the Admin SDK.
"""

import os
import logging
from typing import Any, Dict, Optional, List
import uuid
from datetime import datetime, timezone

import firebase_admin
from firebase_admin import auth as fb_auth, db

from fb_core.real_time_database import FBRTDBMgr

logger = logging.getLogger(__name__)


def _normalize_email(email: Optional[str]) -> Optional[str]:
    """Normalize an email address for case-insensitive comparison.
    
    Args:
        email: Email address to normalize
        
    Returns:
        Normalized email (lowercase and trimmed) or None if invalid
    """
    if email is None:
        return None
    normalized = (email or "").strip().lower()
    if not normalized or "@" not in normalized:
        return None
    return normalized


def _ensure_user_exists_in_auth(customer_email: Optional[str]) -> Optional[str]:
    """Ensure a user exists in Firebase Auth, creating one if necessary.
    
    Looks up user by email. If not found, creates a new user with that email.
    
    Args:
        customer_email: Email address of the user
        
    Returns:
        User UID if successful, None otherwise
    """
    email_value = _normalize_email(customer_email)
    if not email_value:
        logger.warning("Cannot ensure user: invalid email %s", customer_email)
        return None

    try:
        # Try to get existing user by email
        user = fb_auth.get_user_by_email(email_value)
        logger.info("Found existing user for email=%s uid=%s", email_value, user.uid)
        return user.uid
    except fb_auth.UserNotFoundError:
        # User doesn't exist, create one
        try:
            user = fb_auth.create_user(
                email=email_value,
                email_verified=False,
            )
            logger.info("Created new user for email=%s uid=%s", email_value, user.uid)
            return user.uid
        except Exception as create_error:
            logger.warning("Failed to create user for email=%s error=%s", email_value, create_error)
            return None
    except Exception as error:
        logger.warning("Failed to get/create user for email=%s error=%s", email_value, error)
        return None


class FirebaseAdmin:
    """
    Admin wrapper for Firebase operations including RTDB, Auth, and user management.
    
    Provides a high-level interface for:
    - Managing user credit accounts
    - Recording user actions and history
    - Managing output file metadata
    - Atomic credit operations
    """

    def __init__(self, user_id: str, env_id: str = "default"):
        """Initialize FirebaseAdmin for a specific user.
        
        Args:
            user_id: The user identifier (UID or email)
            env_id: Environment identifier (default, staging, etc.)
        """
        self.user_id = user_id
        self.env_id = env_id
        self.database = f"users/{self.user_id}/env/{self.env_id}"
        
        # Initialize database manager
        try:
            firebase_admin.get_app()
            self.db_manager = FBRTDBMgr()
        except ValueError as e:
            logger.warning("Firebase not initialized: %s", e)
            self.db_manager = None

    def create_user_from_google_claims(self, claims: Dict[str, Any]) -> Dict[str, Any]:
        """Create or fetch user from Google authentication claims.
        
        Args:
            claims: Google ID token claims dict
            
        Returns:
            Dict with user info (uid, email, display_name, created, lookup)
        """
        candidate_uid = str(claims.get("sub") or claims.get("uid") or "").strip() or None
        candidate_email = _normalize_email(claims.get("email"))
        
        if not candidate_uid and not candidate_email:
            logger.warning("No uid or email in claims")
            return self._local_user_from_claims(claims)
        
        try:
            # Try to find existing user by UID
            if candidate_uid:
                try:
                    user = fb_auth.get_user(candidate_uid)
                    return {
                        "created": False,
                        "lookup": "uid",
                        "uid": user.uid,
                        "email": user.email,
                        "display_name": user.display_name,
                        "photo_url": user.photo_url,
                        "disabled": user.disabled,
                        "firebase_synced": True,
                    }
                except fb_auth.UserNotFoundError:
                    pass
            
            # Try to find by email
            if candidate_email:
                try:
                    user = fb_auth.get_user_by_email(candidate_email)
                    return {
                        "created": False,
                        "lookup": "email",
                        "uid": user.uid,
                        "email": user.email,
                        "display_name": user.display_name,
                        "photo_url": user.photo_url,
                        "disabled": user.disabled,
                        "firebase_synced": True,
                    }
                except fb_auth.UserNotFoundError:
                    pass
            
            # Create new user with email
            if candidate_email:
                user = fb_auth.create_user(
                    email=candidate_email,
                    display_name=claims.get("name") or claims.get("display_name"),
                    photo_url=claims.get("picture") or claims.get("photoUrl"),
                    email_verified=bool(claims.get("email_verified")),
                )
                logger.info("Created user from Google claims: uid=%s email=%s", user.uid, user.email)
                return {
                    "created": True,
                    "lookup": "email",
                    "uid": user.uid,
                    "email": user.email,
                    "display_name": user.display_name,
                    "photo_url": user.photo_url,
                    "disabled": user.disabled,
                    "firebase_synced": True,
                }
            
            # Fallback to local representation
            return self._local_user_from_claims(claims)
            
        except Exception as error:
            logger.warning("Error creating user from claims: %s", error)
            return self._local_user_from_claims(claims)

    @staticmethod
    def _local_user_from_claims(claims: dict) -> dict:
        """Create a local user representation when Firebase Auth is unavailable."""
        return {
            "created": False,
            "lookup": claims.get("sub") or claims.get("email") or claims.get("uid"),
            "uid": claims.get("sub") or claims.get("uid"),
            "email": claims.get("email"),
            "display_name": claims.get("name") or claims.get("displayName") or claims.get("display_name"),
            "photo_url": claims.get("picture") or claims.get("photoURL") or claims.get("photo_url"),
            "disabled": False,
            "firebase_synced": False,
        }

    def ensure_user_spaces(self) -> bool:
        """Ensure all standard user spaces exist in RTDB.
        
        Creates metadata for credits, usage tracking, output, etc.
        
        Returns:
            True if successful or already exists, False on error
        """
        if self.db_manager is None:
            return False
        
        try:
            # Initialize credits at 0 if not present
            credits_path = f"{self.database}/credits"
            existing_credits = self.db_manager.get_data(credits_path)
            if existing_credits is None:
                self.db_manager.set_data(credits_path, {
                    "balance": 0,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
            
            # Initialize metadata
            metadata_path = f"{self.database}/metadata"
            existing_meta = self.db_manager.get_data(metadata_path)
            if existing_meta is None:
                self.db_manager.set_data(metadata_path, {
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
            
            logger.info("User spaces ensured for user_id=%s", self.user_id)
            return True
        except Exception as error:
            logger.warning("Failed to ensure user spaces for %s: %s", self.user_id, error)
            return False

    def get_credits(self) -> int:
        """Get current credit balance for user.
        
        Returns:
            Credit balance, or 0 if not found/error
        """
        if self.db_manager is None:
            return 0
        
        try:
            credits_path = f"{self.database}/credits"
            data = self.db_manager.get_data(credits_path)
            if isinstance(data, dict):
                return int(data.get("balance") or 0)
            return 0
        except Exception as error:
            logger.warning("Failed to get credits for %s: %s", self.user_id, error)
            return 0

    def add_credits_atomic(self, credits: int, operation_id: str) -> Dict[str, Any]:
        """Atomically add credits to user account.
        
        Args:
            credits: Number of credits to add
            operation_id: Unique operation identifier for idempotency
            
        Returns:
            Result dict with new_balance and transaction_id
        """
        if self.db_manager is None:
            raise ValueError("Database manager not available")
        
        result = {"transaction_id": operation_id, "new_balance": 0}
        
        def _add_credits_txn(current):
            if not isinstance(current, dict):
                current = {"balance": 0}
            
            current_balance = int(current.get("balance") or 0)
            new_balance = current_balance + credits
            
            current["balance"] = new_balance
            current["updated_at"] = datetime.now(timezone.utc).isoformat()
            result["new_balance"] = new_balance
            
            return current
        
        credits_path = f"{self.database}/credits"
        self.db_manager.transact(path=credits_path, update_fn=_add_credits_txn)
        
        # Record transaction
        self._record_transaction("add_credits", credits, operation_id, result.get("new_balance", 0))
        
        logger.info(
            "Credits added user_id=%s credits=%s new_balance=%s op_id=%s",
            self.user_id,
            credits,
            result.get("new_balance"),
            operation_id,
        )
        
        return result

    def deduct_credits_atomic(
        self,
        credits: int,
        operation_id: str,
        require_full_amount: bool = True,
    ) -> Dict[str, Any]:
        """Atomically deduct credits from user account.
        
        Args:
            credits: Number of credits to deduct
            operation_id: Unique operation identifier for idempotency
            require_full_amount: If True, fail if insufficient balance
            
        Returns:
            Result dict with new_balance and transaction_id
            
        Raises:
            ValueError: If insufficient credits and require_full_amount=True
        """
        if self.db_manager is None:
            raise ValueError("Database manager not available")
        
        result = {"transaction_id": operation_id, "new_balance": 0, "deducted": 0}
        
        def _deduct_credits_txn(current):
            if not isinstance(current, dict):
                current = {"balance": 0}
            
            current_balance = int(current.get("balance") or 0)
            
            if current_balance < credits:
                if require_full_amount:
                    result["error"] = "insufficient credits"
                    return current
                # Deduct whatever is available
                deduct_amount = current_balance
            else:
                deduct_amount = credits
            
            new_balance = current_balance - deduct_amount
            current["balance"] = new_balance
            current["updated_at"] = datetime.now(timezone.utc).isoformat()
            result["new_balance"] = new_balance
            result["deducted"] = deduct_amount
            
            return current
        
        credits_path = f"{self.database}/credits"
        self.db_manager.transact(path=credits_path, update_fn=_deduct_credits_txn)
        
        if result.get("error") == "insufficient credits":
            raise ValueError(f"insufficient credits: required {credits}, available {self.get_credits()}")
        
        # Record transaction
        self._record_transaction("deduct_credits", result.get("deducted", 0), operation_id, result.get("new_balance", 0))
        
        logger.info(
            "Credits deducted user_id=%s credits=%s new_balance=%s op_id=%s",
            self.user_id,
            result.get("deducted"),
            result.get("new_balance"),
            operation_id,
        )
        
        return result

    def record_history_event(
        self,
        action: str,
        status: str = "ok",
        details: Optional[Dict[str, Any]] = None,
        request_id: Optional[str] = None,
    ) -> bool:
        """Record a user action in history.
        
        Args:
            action: Action name (e.g., 'generation_success', 'auth.login')
            status: Status of action ('ok', 'error', etc.)
            details: Additional details dict
            request_id: Optional request ID for tracking
            
        Returns:
            True if recorded successfully
        """
        if self.db_manager is None:
            return False
        
        try:
            history_path = f"{self.database}/history"
            event_id = str(uuid.uuid4())
            
            event_data = {
                "event_id": event_id,
                "action": action,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            
            if details and isinstance(details, dict):
                event_data["details"] = details
            
            if request_id:
                event_data["request_id"] = request_id
            
            path = f"{history_path}/{event_id}"
            self.db_manager.set_data(path, event_data)
            
            logger.debug(
                "History event recorded user_id=%s action=%s status=%s",
                self.user_id,
                action,
                status,
            )
            return True
        except Exception as error:
            logger.warning("Failed to record history for %s: %s", self.user_id, error)
            return False

    def ensure_output_space(
        self,
        run_id: str,
        meta: Optional[Dict[str, Any]] = None,
        status: str = "pending",
    ) -> str:
        """Ensure output space exists for a run and update metadata.
        
        Args:
            run_id: Run identifier
            meta: Metadata dict for the run
            status: Run status ('pending', 'running', 'completed', 'error')
            
        Returns:
            The run_id (cleaned)
        """
        if self.db_manager is None:
            return run_id
        
        try:
            cleaned_run_id = str(run_id or "").strip() or str(uuid.uuid4())
            output_path = f"{self.database}/output/{cleaned_run_id}"
            
            run_data = {
                "run_id": cleaned_run_id,
                "status": status,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            
            if meta and isinstance(meta, dict):
                run_data["meta"] = meta
            
            self.db_manager.set_data(output_path, run_data)
            
            logger.debug(
                "Output space ensured user_id=%s run_id=%s status=%s",
                self.user_id,
                cleaned_run_id,
                status,
            )
            return cleaned_run_id
        except Exception as error:
            logger.warning("Failed to ensure output space for %s: %s", self.user_id, error)
            return run_id

    def record_output_files(
        self,
        run_id: str,
        files: List[Dict[str, Any]],
    ) -> bool:
        """Record output files for a run.
        
        Args:
            run_id: Run identifier
            files: List of file dicts with name, mime_type, size_bytes, etc.
            
        Returns:
            True if successful
        """
        if self.db_manager is None:
            return False
        
        try:
            cleaned_run_id = str(run_id or "").strip() or str(uuid.uuid4())
            output_path = f"{self.database}/output/{cleaned_run_id}"
            
            # Build files dict
            files_dict = {}
            for idx, file_item in enumerate(files):
                if not isinstance(file_item, dict):
                    continue
                
                file_id = str(uuid.uuid4())
                files_dict[file_id] = {
                    "name": file_item.get("name"),
                    "mime_type": file_item.get("mime_type"),
                    "size_bytes": file_item.get("size_bytes"),
                    "firebase_path": file_item.get("firebase_path"),
                    "relative_path": file_item.get("relative_path"),
                    "view_url": file_item.get("view_url"),
                    "download_url": file_item.get("download_url"),
                }
            
            update_data = {
                "files": files_dict,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "status": "completed",
            }
            
            self.db_manager.update_data(output_path, update_data)
            
            logger.debug(
                "Output files recorded user_id=%s run_id=%s file_count=%d",
                self.user_id,
                cleaned_run_id,
                len(files_dict),
            )
            return True
        except Exception as error:
            logger.warning("Failed to record output files for %s: %s", self.user_id, error)
            return False

    def _record_transaction(
        self,
        txn_type: str,
        amount: int,
        operation_id: str,
        new_balance: int,
    ) -> bool:
        """Record a transaction in the ledger.
        
        Args:
            txn_type: Type of transaction ('add_credits', 'deduct_credits')
            amount: Amount in transaction
            operation_id: Unique operation ID
            new_balance: Balance after transaction
            
        Returns:
            True if recorded
        """
        if self.db_manager is None:
            return False
        
        try:
            ledger_path = f"{self.database}/ledger"
            txn_data = {
                "type": txn_type,
                "amount": amount,
                "new_balance": new_balance,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operation_id": operation_id,
            }
            
            path = f"{ledger_path}/{operation_id}"
            self.db_manager.set_data(path, txn_data)
            return True
        except Exception as error:
            logger.warning("Failed to record transaction for %s: %s", self.user_id, error)
            return False
