py_files=$(git diff origin/main HEAD --name-only | grep '.py$' | xargs)
pylint "$py_files"
