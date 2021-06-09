#!/usr/bin/env bash
set -ex

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"

cd $DIR

wrgl preview test-base
wrgl diff test-diff-pk-change test-base
wrgl diff test-diff-col-rename test-base
wrgl diff test-diff-col-change test-no-pk
wrgl diff test-diff-no-stat test-base
wrgl diff test-diff-stat test-base
# wrgl merge test-merge-1 test-merge-2