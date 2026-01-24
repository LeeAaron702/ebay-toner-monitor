#!/bin/bash
# Simple sync script for development machines
# Usage:
#   ./sync.sh pull   - Get latest changes before working
#   ./sync.sh push   - Save and upload your work when done
#   ./sync.sh        - Do both (sync everything)

cd "$(dirname "$0")"

pull_changes() {
    echo "📥 Pulling latest changes..."
    git pull origin main
    echo "✅ Up to date!"
}

push_changes() {
    echo "📤 Saving and pushing changes..."
    
    # Add everything including database
    git add -A
    git add -f database.db 2>/dev/null
    
    # Check if there are changes to commit
    if git diff --cached --quiet; then
        echo "ℹ️  No changes to push"
    else
        # Commit with timestamp
        git commit -m "Sync from $(hostname) at $(date '+%Y-%m-%d %H:%M')"
        git push origin main
        echo "✅ Changes pushed!"
    fi
}

case "$1" in
    pull)
        pull_changes
        ;;
    push)
        push_changes
        ;;
    *)
        # Default: pull then push
        pull_changes
        echo ""
        push_changes
        ;;
esac
