#!/usr/bin/env sh
set -e

repo_root=$(git rev-parse --show-toplevel 2>/dev/null)
if [ -z "$repo_root" ]; then
  echo "Error: not inside a git repository." >&2
  exit 1
fi

hooks_dir="$repo_root/.git/hooks"

cat > "$hooks_dir/pre-commit" <<'EOF'
#!/usr/bin/env sh
set -e

# Check formatting before commit
composer format:check
EOF

cat > "$hooks_dir/pre-push" <<'EOF'
#!/usr/bin/env sh
set -e

# Run PHPStan before push
composer analyse
EOF

chmod +x "$hooks_dir/pre-commit" "$hooks_dir/pre-push"

echo "Git hooks installed: pre-commit (format check), pre-push (PHPStan)."
