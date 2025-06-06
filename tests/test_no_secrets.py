"""
Ensure no sensitive credentials or keys are checked into the repository.
This scan runs over all tracked files and fails if any secret patterns are found.
"""
import os
import re
import subprocess
import pytest

# Patterns indicating potential secrets (case-insensitive where appropriate)
SECRET_PATTERNS = [
    re.compile(r'(?i)DB_PASSWORD'),
    re.compile(r'sk-[A-Za-z0-9]{32,}'),      # OpenAI secret key pattern
    re.compile(r'AKIA[0-9A-Z]{16}'),         # AWS access key ID
    re.compile(r'(?i)SECRET_ACCESS_KEY'),
    re.compile(r'-----BEGIN (?:RSA )?PRIVATE KEY-----'),
]

def test_no_secrets_in_repo():
    # Get list of tracked files via git, fallback to walking filesystem
    try:
        result = subprocess.run(
            ['git', 'ls-files'], capture_output=True, text=True, check=True
        )
        files = result.stdout.splitlines()
    except Exception:
        files = []
        for root, _, filenames in os.walk(os.getcwd()):
            for fn in filenames:
                files.append(os.path.relpath(os.path.join(root, fn), os.getcwd()))

    secrets_found = []
    for filepath in files:
        # Skip git internals and example env files
        if filepath.startswith('.git/'):
            continue
        if os.path.basename(filepath) in ('.env', '.env.example'):
            continue
        fullpath = os.path.join(os.getcwd(), filepath)
        if not os.path.isfile(fullpath):
            continue
        try:
            with open(fullpath, 'r', encoding='utf-8') as f:
                for lineno, line in enumerate(f, start=1):
                    if line.strip().startswith('#'):
                        continue
                    for pattern in SECRET_PATTERNS:
                        if pattern.search(line):
                            secrets_found.append(f"{filepath}:{lineno}: {line.strip()}")
        except (UnicodeDecodeError, PermissionError):
            continue

    assert not secrets_found, (
        "Sensitive patterns found in repository files:\n" + "\n".join(secrets_found)
    )