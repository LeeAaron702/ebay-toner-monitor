#!/usr/bin/env python3
"""
Git Auto-Pull for Server Deployment (CD - Continuous Deployment)

For SERVERS only - automatically pulls latest changes every hour.
Development machines should use ./sync.sh instead.

Usage:
    python scripts/git_sync.py           # Run once (pull only)
    python scripts/git_sync.py --daemon  # Run as background auto-puller
    
Environment Variables:
    GIT_AUTO_SYNC=true          Enable auto-sync (required for daemon)
    GIT_SYNC_BRANCH=main        Branch to sync (default: main)
    GIT_SYNC_INTERVAL=3600      Seconds between syncs (default: 1 hour)
"""

import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Configuration
REPO_PATH = Path(__file__).parent.parent
GIT_BRANCH = os.getenv("GIT_SYNC_BRANCH", "main")
SYNC_INTERVAL = int(os.getenv("GIT_SYNC_INTERVAL", "3600"))  # 1 hour default


def _log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


def run_git(args: list[str]) -> subprocess.CompletedProcess:
    """Run a git command."""
    cmd = ["git", "-C", str(REPO_PATH)] + args
    return subprocess.run(cmd, capture_output=True, text=True)


def pull() -> bool:
    """Pull latest changes from remote."""
    _log("Checking for updates...")
    
    # Fetch
    result = run_git(["fetch", "origin", GIT_BRANCH])
    if result.returncode != 0:
        _log(f"Fetch failed: {result.stderr}")
        return False
    
    # Check if behind
    result = run_git(["rev-list", "--count", f"HEAD..origin/{GIT_BRANCH}"])
    commits_behind = int(result.stdout.strip()) if result.returncode == 0 else 0
    
    if commits_behind == 0:
        _log("Already up to date.")
        return True
    
    _log(f"Behind by {commits_behind} commit(s), pulling...")
    
    # Reset to remote (clean pull, server shouldn't have local changes)
    result = run_git(["reset", "--hard", f"origin/{GIT_BRANCH}"])
    if result.returncode != 0:
        _log(f"Reset failed: {result.stderr}")
        return False
    
    _log(f"✅ Pulled {commits_behind} commit(s)")
    return True


def daemon():
    """Run as background daemon, pulling every SYNC_INTERVAL seconds."""
    _log("=" * 50)
    _log("Starting Git Auto-Pull Daemon (Server Mode)")
    _log(f"Branch: {GIT_BRANCH}")
    _log(f"Interval: {SYNC_INTERVAL} seconds ({SYNC_INTERVAL // 60} minutes)")
    _log("=" * 50)
    
    # Initial pull
    pull()
    
    while True:
        time.sleep(SYNC_INTERVAL)
        _log("-" * 30)
        pull()


def main():
    if "--daemon" in sys.argv:
        auto_sync = os.getenv("GIT_AUTO_SYNC", "false").lower() in ("true", "1", "yes")
        if not auto_sync:
            _log("WARNING: GIT_AUTO_SYNC not enabled, but running daemon anyway...")
        daemon()
    elif "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
    else:
        # Single pull
        success = pull()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
