# WRGL

Git-like data versioning. It can handle files up to 10s of Gigabytes. How it differs to other alternative such as [dolt](https://github.com/dolthub/dolt) is that it doesn't require a schema up front, any arbitrary CSV file can be commited and that it can display much more detailed diff.

## Installation

```bash
sudo bash -c 'curl -L https://github.com/wrgl/core/releases/latest/download/install.sh | bash'
```

## Usage

```bash
# initialize repository. This will create a .wrgl folder
wrgl init

# create a new branch by committing a CSV file
wrgl commit my-branch my_data.csv --primary-key id

# create another commit under the same branch
wrgl commit my-branch my_other_data.csv --primary-key id

# show diff between the last 2 commits
wrgl diff my-branch my-branch^

# output diff to JSON stream
wrgl diff my-branch my-branch^ --format json

# display list of commits within a branch
wrgl log my-branch

# preview data withint a commit
wrgl preview my-branch~2

# export data back to CSV
wrgl export my-branch > data.csv

# list branches
wrgl branch

# delete branch
wrgl branch -d my-branch
```

## Dealing with big file

There are 2 storage formats for table: `small` and `big`. For files that can easily fit in memory (up to a few Gigabytes in size), `small` table format should be used. This is the default storage mode. For larger files, use `big` format instead. `big` table operations interact with disk more and are therefore slower but require less memory than `small` tables for the same file size. Add flag `--big-table` during commit to use `big` format:

```bash
# Make sure to set ulimit to something large if you're dealing with a large file
ulimit -n 10000

# Add flag --big-table during commit
wrgl commit my-branch big_file.csv --primary-key my_key --big-table
```

## Roadmap

- Add ability to setup remote and sync files between local and remotes just like Git
