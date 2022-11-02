#!/usr/bin/env bash
set -e

if [ "$#" -lt 1 ]; then
    echo "Invalid number of arguments"
    echo "Usage: prepare_test_data.sh FILE_NAME [FRACTION]"
    exit 1
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )/.." >/dev/null 2>&1 && pwd )"
FILE="$1"
FRACTION="${2-0.2}"

cd $DIR

if [ -d ".wrgl" ]; then
    echo "Wrgl repo already exists. Exiting."
    exit 0
fi

wrgl init
csvgen $FILE | wrgl commit test-no-pk - "first commit"
csvgen $FILE | wrgl commit test-base - "first commit" -p col_a
csvgen $FILE -f $FRACTION --addrem-cols --preserve-cols col_a,col_b | wrgl commit test-diff-pk-change - "first commit" -p col_b
csvgen $FILE -f $FRACTION --rename-cols --preserve-cols col_a | wrgl commit test-diff-col-rename - "first commit" -p col_a
csvgen $FILE -f $FRACTION --addrem-cols | wrgl commit test-diff-col-change - "first commit"
csvgen $FILE -f $FRACTION --mod-rows --preserve-cols col_a | wrgl commit test-diff-no-stat - "first commit" -p col_a
csvgen $FILE -f $FRACTION --mod-rows --move-cols --addrem-cols --preserve-cols col_a | wrgl commit test-diff-stat - "first commit" -p col_a
wrgl branch create -c test-merge-1 test-base
wrgl branch create -c test-merge-2 test-base
csvgen $FILE -f $FRACTION --mod-rows --move-cols --addrem-cols --preserve-cols col_a | wrgl commit test-merge-1 - "second commit" -p col_a
csvgen $FILE -f $FRACTION --mod-rows --move-cols --addrem-cols --preserve-cols col_a | wrgl commit test-merge-2 - "second commit" -p col_a