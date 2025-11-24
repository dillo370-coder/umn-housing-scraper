#!/usr/bin/env bash
# Simple repo setup script for ~/Documents/umn-housing-scraper
# Copy this file into the project root, then run:
#   chmod +x setup_repo.sh
#   ./setup_repo.sh
set -euo pipefail

ROOT="$HOME/Documents/umn-housing-scraper"

if [ ! -d "$ROOT" ]; then
  echo "Directory not found: $ROOT"
  exit 1
fi

cd "$ROOT"
echo "Working in: $(pwd)"
echo

# 1) Ensure scraper is a package and remove accidental file if present
if [ -f "scraper/init.py" ]; then
  rm -f "scraper/init.py"
  echo "Removed scraper/init.py"
fi
mkdir -p scraper
if [ ! -f "scraper/__init__.py" ]; then
  touch "scraper/__init__.py"
  echo "Created scraper/__init__.py"
fi
echo "scraper/ contents:"
ls -la scraper || true
echo

# 2) Move your versioned files into canonical names if they exist
mkdir -p .github/workflows
if [ -f "github_workflows_ci_Version3.yml" ]; then
  mv "github_workflows_ci_Version3.yml" .github/workflows/ci.yml
  echo "Moved github_workflows_ci_Version3.yml -> .github/workflows/ci.yml"
fi
if [ -f "README_Version3.md" ]; then
  mv "README_Version3.md" "README.md"
  echo "Moved README_Version3.md -> README.md"
fi
if [ -f "requirements_Version2.txt" ]; then
  mv "requirements_Version2.txt" "requirements.txt"
  echo "Moved requirements_Version2.txt -> requirements.txt"
fi
if [ -f "LICENSE_Version3.txt" ]; then
  mv "LICENSE_Version3.txt" "LICENSE"
  echo "Moved LICENSE_Version3.txt -> LICENSE"
fi
echo

# 3) Write a safe .gitignore (overwrites if present)
cat > .gitignore <<'GITIGNORE'
__pycache__/
*.py[cod]
.env
.venv
venv/
.vscode/
.idea/
.DS_Store
.playwright/
output/
*.log
*.csv
geocode_cache.json
*.bak
GITIGNORE
echo "Wrote .gitignore"
echo

# 4) Initialize git repository if needed
if [ ! -d ".git" ]; then
  git init
  echo "Initialized new git repository"
else
  echo "Git repository already exists"
fi

# 5) Stage files
git add .

# 6) Commit if there are staged changes
if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git commit -m "Initial commit: add scraper project"
  echo "Committed files"
fi
echo

# 7) Add remote if not present
REMOTE_URL="https://github.com/dillo370-coder/umn-housing-scraper.git"
if git remote get-url origin >/dev/null 2>&1; then
  echo "Remote origin already set to: $(git remote get-url origin)"
else
  git remote add origin "$REMOTE_URL"
  echo "Added remote origin -> $REMOTE_URL"
fi
echo

# 8) Ensure branch name main and attempt push
git branch -M main
echo "Attempting to push to origin main. You may be prompted for credentials."
if git push -u origin main; then
  echo "Push successful."
else
  echo "Push failed. Please authenticate (recommended: run 'gh auth login') or provide a PAT when prompted."
  echo "After authenticating run: git push -u origin main"
fi

echo
echo "Setup script complete."
echo "Verify with: git status ; git remote -v"
echo "If push failed due to authentication, run: gh auth login  (or use PAT), then re-run: git push -u origin main"#!/usr/bin/env bash
# Simple repo setup script for ~/Documents/umn-housing-scraper
# Copy this file into the project root, then run:
#   chmod +x setup_repo.sh
#   ./setup_repo.sh
set -euo pipefail

ROOT="$HOME/Documents/umn-housing-scraper"

if [ ! -d "$ROOT" ]; then
  echo "Directory not found: $ROOT"
  exit 1
fi

cd "$ROOT"
echo "Working in: $(pwd)"
echo

# 1) Ensure scraper is a package and remove accidental file if present
if [ -f "scraper/init.py" ]; then
  rm -f "scraper/init.py"
  echo "Removed scraper/init.py"
fi
mkdir -p scraper
if [ ! -f "scraper/__init__.py" ]; then
  touch "scraper/__init__.py"
  echo "Created scraper/__init__.py"
fi
echo "scraper/ contents:"
ls -la scraper || true
echo

# 2) Move your versioned files into canonical names if they exist
mkdir -p .github/workflows
if [ -f "github_workflows_ci_Version3.yml" ]; then
  mv "github_workflows_ci_Version3.yml" .github/workflows/ci.yml
  echo "Moved github_workflows_ci_Version3.yml -> .github/workflows/ci.yml"
fi
if [ -f "README_Version3.md" ]; then
  mv "README_Version3.md" "README.md"
  echo "Moved README_Version3.md -> README.md"
fi
if [ -f "requirements_Version2.txt" ]; then
  mv "requirements_Version2.txt" "requirements.txt"
  echo "Moved requirements_Version2.txt -> requirements.txt"
fi
if [ -f "LICENSE_Version3.txt" ]; then
  mv "LICENSE_Version3.txt" "LICENSE"
  echo "Moved LICENSE_Version3.txt -> LICENSE"
fi
echo

# 3) Write a safe .gitignore (overwrites if present)
cat > .gitignore <<'GITIGNORE'
__pycache__/
*.py[cod]
.env
.venv
venv/
.vscode/
.idea/
.DS_Store
.playwright/
output/
*.log
*.csv
geocode_cache.json
*.bak
GITIGNORE
echo "Wrote .gitignore"
echo

# 4) Initialize git repository if needed
if [ ! -d ".git" ]; then
  git init
  echo "Initialized new git repository"
else
  echo "Git repository already exists"
fi

# 5) Stage files
git add .

# 6) Commit if there are staged changes
if git diff --cached --quiet; then
  echo "No changes to commit."
else
  git commit -m "Initial commit: add scraper project"
  echo "Committed files"
fi
echo

# 7) Add remote if not present
REMOTE_URL="https://github.com/dillo370-coder/umn-housing-scraper.git"
if git remote get-url origin >/dev/null 2>&1; then
  echo "Remote origin already set to: $(git remote get-url origin)"
else
  git remote add origin "$REMOTE_URL"
  echo "Added remote origin -> $REMOTE_URL"
fi
echo

# 8) Ensure branch name main and attempt push
git branch -M main
echo "Attempting to push to origin main. You may be prompted for credentials."
if git push -u origin main; then
  echo "Push successful."
else
  echo "Push failed. Please authenticate (recommended: run 'gh auth login') or provide a PAT when prompted."
  echo "After authenticating run: git push -u origin main"
fi

echo
echo "Setup script complete."
echo "Verify with: git status ; git remote -v"
echo "If push failed due to authentication, run: gh auth login  (or use PAT), then re-run: git push -u origin main"
