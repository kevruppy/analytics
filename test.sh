files=$(git diff --name-only --cached | grep '.py$')
echo "Found files:$files"
if [[ -n "$files" ]]; then
    echo "HI"
else
    echo "No .py files to be linted found. Skipping Pylint."
fi
