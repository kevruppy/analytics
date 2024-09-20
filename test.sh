py_files=$(git rev-parse --show-toplevel && git diff origin/main HEAD --name-only | grep '.py$' | xargs)
pylint "$py_files"
