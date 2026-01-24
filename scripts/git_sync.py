#!/usr/bin/env python3
"""
Git Auto-Sync Script for Multi-Machine Deployments

This script runs automatically 2x per day (12:00 PM and 11:59 PM) to:
1. Pull latest changes from remote (handles conflicts)
2. Commit local changes (database, config changes)
3. Push to remote repository

Designed for seamless sync across multiple machines running the same container.

Usage:
    python scripts/git_sync.py           # Run once
    python scripts/git_sync.py --daemon  # Run as background scheduler
    
Environment Variables:
    GIT_AUTO_SYNC=true          Enable auto-sync (default: false)
    GIT_SYNC_BRANCH=main        Branch to sync (default: main)
    GIT_REMOTE=origin           Remote name (default: origin)
    GIT_USER_NAME=Bot           Git commit author name
    GIT_USER_EMAIL=bot@local    Git commit author email
    MACHINE_ID=machine-1        Identifier for this machine in commits
"""

import os
import subprocess
import sys
import logging
from datetime import datetime
from pathlib import Path
import socket
import hashlib

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from utils.base import _log
except ImportError:
    # Fallback if utils not available
    def _log(msg):
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")

# Configuration
REPO_PATH = Path(__file__).parent.parent
GIT_BRANCH = os.getenv("GIT_SYNC_BRANCH", "main")
GIT_REMOTE = os.getenv("GIT_REMOTE", "origin")
GIT_USER_NAME = os.getenv("GIT_USER_NAME", "Toner Monitor Bot")
GIT_USER_EMAIL = os.getenv("GIT_USER_EMAIL", "bot@toner-monitor.local")
MACHINE_ID = os.getenv("MACHINE_ID", socket.gethostname()[:12])

# Files to always include in sync (even if in .gitignore)
SYNC_FILES = [
    "database.db",
]

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_git_command(args: list[str], check: bool = True) -> subprocess.CompletedProcess:
    """Run a git command and return the result."""
    cmd = ["git", "-C", str(REPO_PATH)] + args
    _log(f"Running: {' '.join(cmd)}")
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        check=False
    )
    
    if result.returncode != 0 and check:
        _log(f"Git command failed: {result.stderr}")
        
    return result


def get_file_hash(filepath: Path) -> str | None:
    """Get SHA256 hash of a file for change detection."""
    if not filepath.exists():
        return None
    try:
        with open(filepath, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:12]
    except Exception:
        return None


def is_git_repo() -> bool:
    """Check if we're in a git repository."""
    result = run_git_command(["rev-parse", "--is-inside-work-tree"], check=False)
    return result.returncode == 0


def has_remote() -> bool:
    """Check if remote is configured."""
    result = run_git_command(["remote", "get-url", GIT_REMOTE], check=False)
    return result.returncode == 0


def get_current_branch() -> str:
    """Get the current branch name."""
    result = run_git_command(["rev-parse", "--abbrev-ref", "HEAD"])
    return result.stdout.strip() if result.returncode == 0 else GIT_BRANCH


def configure_git_user():
    """Configure git user for commits."""
    run_git_command(["config", "user.name", GIT_USER_NAME], check=False)
    run_git_command(["config", "user.email", GIT_USER_EMAIL], check=False)


def stash_local_changes() -> bool:
    """Stash local changes before pull."""
    result = run_git_command(["stash", "push", "-m", f"auto-stash-{MACHINE_ID}-{datetime.now().isoformat()}"])
    return result.returncode == 0


def pop_stash() -> bool:
    """Pop stashed changes after pull."""
    result = run_git_command(["stash", "pop"], check=False)
    return result.returncode == 0


def has_local_changes() -> bool:
    """Check if there are uncommitted changes."""
    result = run_git_command(["status", "--porcelain"])
    return bool(result.stdout.strip())


def force_track_files():
    """Force-add files that might be in .gitignore but should be synced."""
    for filename in SYNC_FILES:
        filepath = REPO_PATH / filename
        if filepath.exists():
            # Use -f to force add even if in .gitignore
            run_git_command(["add", "-f", filename], check=False)
            _log(f"Force-tracked: {filename}")


def pull_changes() -> tuple[bool, str]:
    """
    Pull latest changes from remote.
    For database conflicts, keeps LOCAL version (ours) since we commit first.
    
    Returns:
        (success: bool, message: str)
    """
    _log(f"Pulling from {GIT_REMOTE}/{GIT_BRANCH}...")
    
    # Fetch first to see what's available
    fetch_result = run_git_command(["fetch", GIT_REMOTE, GIT_BRANCH], check=False)
    if fetch_result.returncode != 0:
        return False, f"Fetch failed: {fetch_result.stderr}"
    
    # Check if we're behind
    result = run_git_command(["rev-list", "--count", f"HEAD..{GIT_REMOTE}/{GIT_BRANCH}"], check=False)
    commits_behind = int(result.stdout.strip()) if result.returncode == 0 else 0
    
    if commits_behind == 0:
        _log("Already up to date.")
        return True, "Already up to date"
    
    _log(f"Behind by {commits_behind} commit(s), pulling...")
    
    # Pull with strategy to handle conflicts
    # Use "ours" strategy for database since we committed our changes first
    pull_result = run_git_command(["pull", "--no-rebase", GIT_REMOTE, GIT_BRANCH], check=False)
    
    if pull_result.returncode != 0:
        # Check for merge conflicts
        if "CONFLICT" in pull_result.stdout or "conflict" in pull_result.stderr.lower():
            _log("Merge conflict detected, resolving...")
            
            # For database.db, keep OUR version (we committed first, so ours has our changes)
            for db_file in ["database.db"]:
                db_path = REPO_PATH / db_file
                if db_path.exists():
                    run_git_command(["checkout", "--ours", db_file], check=False)
                    run_git_command(["add", db_file], check=False)
                    _log(f"Kept local version of {db_file}")
            
            # For code files, prefer remote (theirs) to get latest code updates
            # Get list of conflicted files
            status_result = run_git_command(["diff", "--name-only", "--diff-filter=U"], check=False)
            conflicted_files = status_result.stdout.strip().split('\n') if status_result.stdout.strip() else []
            
            for f in conflicted_files:
                if f and f != "database.db":
                    run_git_command(["checkout", "--theirs", f], check=False)
                    run_git_command(["add", f], check=False)
                    _log(f"Kept remote version of {f}")
            
            # Complete the merge
            run_git_command(["commit", "-m", f"Auto-merge from {MACHINE_ID}: kept local database, remote code"], check=False)
            
            return True, f"Pulled {commits_behind} commit(s), resolved conflicts"
        
        return False, f"Pull failed: {pull_result.stderr}"
    
    return True, f"Pulled {commits_behind} commit(s)"


def commit_changes() -> tuple[bool, str]:
    """
    Stage and commit local changes.
    
    Returns:
        (success: bool, message: str)
    """
    if not has_local_changes():
        _log("No local changes to commit.")
        return True, "No changes"
    
    # Force-track important files
    force_track_files()
    
    # Stage all changes
    run_git_command(["add", "-A"])
    
    # Generate commit message with details
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    db_hash = get_file_hash(REPO_PATH / "database.db") or "none"
    
    commit_msg = f"Auto-sync from {MACHINE_ID} at {timestamp}\n\nDatabase hash: {db_hash}"
    
    result = run_git_command(["commit", "-m", commit_msg], check=False)
    
    if result.returncode != 0:
        if "nothing to commit" in result.stdout:
            return True, "Nothing to commit"
        return False, f"Commit failed: {result.stderr}"
    
    _log("Changes committed successfully.")
    return True, "Committed"


def push_changes() -> tuple[bool, str]:
    """
    Push commits to remote.
    
    Returns:
        (success: bool, message: str)
    """
    _log(f"Pushing to {GIT_REMOTE}/{GIT_BRANCH}...")
    
    result = run_git_command(["push", GIT_REMOTE, GIT_BRANCH], check=False)
    
    if result.returncode != 0:
        # Check if we need to pull first
        if "non-fast-forward" in result.stderr or "rejected" in result.stderr:
            _log("Push rejected, need to pull first...")
            return False, "Push rejected - need to pull"
        
        return False, f"Push failed: {result.stderr}"
    
    _log("Push successful!")
    return True, "Pushed"


def sync() -> dict:
    """
    Main sync function: COMMIT -> PULL -> PUSH
    
    Order is critical:
    1. Commit local changes first (so they're in git history)
    2. Pull remote changes (conflicts resolved keeping our database)
    3. Push our commits to remote
    
    This ensures local database changes are never lost during pull.
    
    Returns:
        dict with sync status and messages
    """
    _log("=" * 50)
    _log(f"Starting Git Sync - Machine: {MACHINE_ID}")
    _log("=" * 50)
    
    result = {
        "success": False,
        "machine_id": MACHINE_ID,
        "timestamp": datetime.now().isoformat(),
        "commit": None,
        "pull": None,
        "push": None,
        "error": None
    }
    
    # Validate git setup
    if not is_git_repo():
        result["error"] = "Not a git repository"
        _log(f"ERROR: {result['error']}")
        return result
    
    if not has_remote():
        result["error"] = f"Remote '{GIT_REMOTE}' not configured"
        _log(f"ERROR: {result['error']}")
        return result
    
    # Configure git user
    configure_git_user()
    
    max_retries = 3
    
    # Step 1: COMMIT local changes first (critical - protects local db from being overwritten)
    commit_success, commit_msg = commit_changes()
    result["commit"] = commit_msg
    _log(f"Commit result: {commit_msg}")
    
    # Step 2: PULL latest changes (after committing, so we can resolve conflicts properly)
    for attempt in range(max_retries):
        pull_success, pull_msg = pull_changes()
        result["pull"] = pull_msg
        
        if pull_success:
            break
        elif attempt < max_retries - 1:
            _log(f"Pull attempt {attempt + 1} failed, retrying...")
        else:
            result["error"] = f"Pull failed after {max_retries} attempts: {pull_msg}"
            _log(f"ERROR: {result['error']}")
            return result
    
    # Step 3: PUSH changes (with retry loop for concurrent pushes)
    for attempt in range(max_retries):
        push_success, push_msg = push_changes()
        result["push"] = push_msg
        
        if push_success:
            break
        elif "need to pull" in push_msg.lower() or "rejected" in push_msg.lower():
            # Another machine pushed while we were syncing, pull and try again
            _log("Remote has new commits, pulling and retrying push...")
            pull_changes()
        elif attempt < max_retries - 1:
            _log(f"Push attempt {attempt + 1} failed, retrying...")
        else:
            result["error"] = f"Push failed after {max_retries} attempts: {push_msg}"
            _log(f"ERROR: {result['error']}")
            return result
    
    result["success"] = True
    _log("=" * 50)
    _log("Git Sync Complete!")
    _log("=" * 50)
    
    return result


def run_daemon():
    """Run as a background daemon with scheduled sync at 6:00 AM and 6:00 PM."""
    try:
        import schedule
        import time
    except ImportError:
        _log("ERROR: 'schedule' package not installed. Run: pip install schedule")
        sys.exit(1)
    
    _log("Starting Git Sync Daemon...")
    _log(f"Machine ID: {MACHINE_ID}")
    _log(f"Branch: {GIT_BRANCH}")
    _log(f"Remote: {GIT_REMOTE}")
    _log("Scheduled sync times: 6:00 AM and 6:00 PM")
    
    # Schedule sync at 6:00 AM
    schedule.every().day.at("06:00").do(sync)
    
    # Schedule sync at 6:00 PM
    schedule.every().day.at("18:00").do(sync)
    
    # Run initial sync on startup
    _log("Running initial sync...")
    sync()
    
    # Keep running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute


def main():
    """Main entry point."""
    # Check if auto-sync is enabled
    auto_sync_enabled = os.getenv("GIT_AUTO_SYNC", "false").lower() in ("true", "1", "yes")
    
    if "--daemon" in sys.argv:
        if not auto_sync_enabled:
            _log("WARNING: GIT_AUTO_SYNC is not enabled. Set GIT_AUTO_SYNC=true to enable.")
            _log("Running anyway since --daemon flag was passed...")
        run_daemon()
    elif "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
    else:
        # Single run
        result = sync()
        if not result["success"]:
            sys.exit(1)


if __name__ == "__main__":
    main()
