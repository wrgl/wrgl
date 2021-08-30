# WRGL

Git-like data versioning. It can handle files up to 10s of Gigabytes.

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

## Version compatibility

This software isn't ready for production so all new minor version introduce breaking changes. You will need to throw away the entire `.wrgl` folder once you upgrade to a new minor version e.g. `0.1.x` to `0.2.x`.
