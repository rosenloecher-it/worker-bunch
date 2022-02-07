#!/usr/bin/env bash
set -e

function clean_build_dirs {
  rm -rf ./build > /dev/null 2>&1
  rm -rf ./dist > /dev/null 2>&1
  rm -rf ./worker_bunch.egg-info > /dev/null 2>&1
}

# change into script dir to use relative paths
SCRIPT_PATH=$(readlink -f $0)
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
SCRIPT_NAME=$(basename $0)
cd "$SCRIPT_DIR"

export PYTHONPATH="$SCRIPT_DIR"
export PYTHONUNBUFFERED=1

if [[ "$VIRTUAL_ENV" != "" ]] ; then
  # no active virtual env => activate it
  VENV_ACTIVATE="./venv/bin/activate"
  if [ ! -f "$VENV_ACTIVATE" ] ; then
    echo -e "$SCRIPT_NAME\nerror: venv environment doesn't exist!"
    exit 1
  fi

  source "$VENV_ACTIVATE"
  RC=$?
  if [ $RC -ne 0 ] ; then
    echo -e "$SCRIPT_NAME\nerror: activating environment failed!"
    exit 1
  fi
fi

clean_build_dirs

python setup.py sdist bdist_wheel
python -m twine upload --repository pypi dist/*

clean_build_dirs

echo "done"

exit 0
