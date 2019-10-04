import os

'''
On branch master
Your branch is up to date with 'origin/master'.

Changes not staged for commit:
  (use "git add <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   modules/Messages.py
	modified:   weaviate-cli.py

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	modules/upgrade.py

no changes added to commit (use "git add" and/or "git commit -a")
'''

'''
On branch master
Your branch is ahead of 'origin/master' by 1 commit.
  (use "git push" to publish your local commits)

nothing to commit, working tree clean

'''

from modules.Messages import Messages

def upgrade_weaviate_cli(entry_point):
    entry_point = entry_point[:-16]
    if os.system('git --version') != 0:
        print("The upgrade routine uses git. If you installed the weaviate-cli manually please upgrade manually.")
        exit(1)
    if os.system('git remote update') !=0:
        print("Sorry but the upgrade failed in the git update")
    

    exit(0)