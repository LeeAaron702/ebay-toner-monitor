#!/bin/bash
#
# GitHub Repository Setup Script
# ==============================
# This script initializes the git repository and prepares it for multi-machine sync.
#
# Usage:
#   ./scripts/setup_github.sh <github-repo-url>
#
# Example:
#   ./scripts/setup_github.sh git@github.com:username/ebay-toner-monitor.git
#   ./scripts/setup_github.sh https://github.com/username/ebay-toner-monitor.git
#

set -e

REPO_URL="$1"
BRANCH="${GIT_SYNC_BRANCH:-main}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}  eBay Toner Monitor - GitHub Setup${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Check if repo URL provided
if [ -z "$REPO_URL" ]; then
    echo -e "${YELLOW}Usage: $0 <github-repo-url>${NC}"
    echo ""
    echo "First, create a new repository on GitHub:"
    echo "  1. Go to https://github.com/new"
    echo "  2. Create a new PRIVATE repository (recommended)"
    echo "  3. Do NOT initialize with README, .gitignore, or license"
    echo "  4. Copy the SSH or HTTPS URL"
    echo ""
    echo "Then run:"
    echo "  $0 git@github.com:YOUR_USERNAME/YOUR_REPO.git"
    exit 1
fi

# Change to script directory's parent (repo root)
cd "$(dirname "$0")/.."
REPO_ROOT=$(pwd)

echo -e "${GREEN}Repository root: $REPO_ROOT${NC}"
echo ""

# Check if already a git repo
if [ -d ".git" ]; then
    echo -e "${YELLOW}Git repository already exists.${NC}"
    
    # Check current remote
    CURRENT_REMOTE=$(git remote get-url origin 2>/dev/null || echo "none")
    
    if [ "$CURRENT_REMOTE" != "none" ]; then
        echo -e "Current remote: ${BLUE}$CURRENT_REMOTE${NC}"
        
        if [ "$CURRENT_REMOTE" != "$REPO_URL" ]; then
            echo -e "${YELLOW}Updating remote to: $REPO_URL${NC}"
            git remote set-url origin "$REPO_URL"
        fi
    else
        echo -e "Adding remote: ${BLUE}$REPO_URL${NC}"
        git remote add origin "$REPO_URL"
    fi
else
    echo -e "${GREEN}Initializing new git repository...${NC}"
    git init
    git remote add origin "$REPO_URL"
fi

# Configure git
echo ""
echo -e "${GREEN}Configuring git...${NC}"

# Set default branch
git config init.defaultBranch "$BRANCH"

# Check if user is configured
if [ -z "$(git config user.name)" ]; then
    echo -e "${YELLOW}Git user.name not set. Please configure:${NC}"
    echo "  git config user.name 'Your Name'"
fi

if [ -z "$(git config user.email)" ]; then
    echo -e "${YELLOW}Git user.email not set. Please configure:${NC}"
    echo "  git config user.email 'your@email.com'"
fi

# Force-add important files that might be in .gitignore
echo ""
echo -e "${GREEN}Staging files for initial commit...${NC}"

# Add all files
git add -A

# Force-add database if it exists
if [ -f "database.db" ]; then
    echo -e "  Adding database.db (for version history)"
    git add -f database.db
fi

# Check if there are files to commit
if git diff --cached --quiet; then
    echo -e "${YELLOW}No new files to commit.${NC}"
else
    echo ""
    echo -e "${GREEN}Creating initial commit...${NC}"
    git commit -m "Initial commit: eBay Toner Arbitrage Monitor

Includes:
- FastAPI server with Telegram webhook
- Canon, Xerox, Lexmark monitoring engines
- SQLite database with product catalog
- Multi-machine git sync support"
fi

# Set upstream branch
echo ""
echo -e "${GREEN}Setting up branch '$BRANCH'...${NC}"

# Check if branch exists
if git show-ref --verify --quiet "refs/heads/$BRANCH"; then
    echo -e "Branch '$BRANCH' already exists"
else
    git branch -M "$BRANCH"
fi

# Push to remote
echo ""
echo -e "${GREEN}Pushing to GitHub...${NC}"
echo -e "${YELLOW}(You may be prompted for credentials)${NC}"
echo ""

if git push -u origin "$BRANCH"; then
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}  ✓ Setup Complete!${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "Repository: ${BLUE}$REPO_URL${NC}"
    echo -e "Branch: ${BLUE}$BRANCH${NC}"
    echo ""
    echo -e "${YELLOW}Next steps:${NC}"
    echo "  1. Enable git sync in .env:"
    echo "     GIT_AUTO_SYNC=true"
    echo "     GIT_SYNC_BRANCH=$BRANCH"
    echo ""
    echo "  2. Start with Docker:"
    echo "     docker-compose up -d --build"
    echo ""
    echo "  3. On other machines, clone and configure:"
    echo "     git clone $REPO_URL"
    echo "     cp .env.example .env"
    echo "     # Edit .env with credentials"
    echo "     docker-compose up -d --build"
    echo ""
else
    echo ""
    echo -e "${RED}========================================${NC}"
    echo -e "${RED}  ✗ Push Failed${NC}"
    echo -e "${RED}========================================${NC}"
    echo ""
    echo -e "${YELLOW}Troubleshooting:${NC}"
    echo "  - For SSH: Make sure your SSH key is added to GitHub"
    echo "    ssh-keygen -t ed25519 -C 'your@email.com'"
    echo "    cat ~/.ssh/id_ed25519.pub  # Add this to GitHub"
    echo ""
    echo "  - For HTTPS: You may need a Personal Access Token"
    echo "    https://github.com/settings/tokens"
    echo ""
    echo "  - Make sure the repository exists on GitHub"
    echo ""
    exit 1
fi
