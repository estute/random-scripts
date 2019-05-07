import os
import sys
from collections import namedtuple

# CONDITIONS:
## 1) go into a node, fill batch, close off batch
## 2) go into a node, add files, proceed to next batch, fill, and close off
## 3) go into a node, fill batch, but determine that subdirs exist and are batched, close off and mark as not ready for work
# 4) go into a node, fill batch, mark as overflow
# 5) go into a node, fill batch, determine that can't be balanced well




MAX_FILE_NUMBER = 15


class Batch(object):
    """
    representation of a `batch` of python files, pulled
    """

    def __init__(self, root):
        self.root = root
        self.files = []
        self.blocked = False

    def __str__(self):
        return "{} :: {}".format(self.root, len(self.files))

    def add(self, file_path):
        self.files.append(file_path)
        self.rebalance_root()

    def contains_file(self, file_path):
        return file_path in self.files

    def contains_dir(self, dir_path):
        return dir_path in self.directories

    @property
    def directories(self):
        """
        return a list of all of the directories cotained within this batch
        of files.
        """
        directories = list(set([
            '/'.join(f.split('/')[:-1]) for f in self.files
        ]))
        return directories

    @property
    def top_level_directories(self):
        """
        return a list of all of the top level directories in this batch of
        files, that is, all of the directories that are not contained in other
        directories in this batch
        """
        top_level_directories = filter(
            lambda d: len([x for x in self.directories if x in d]) == 1,
            self.directories
        )
        return top_level_directories

    def rebalance_root(self):
        """
        """
        # if len(self.directories) < 2:
            # self.root = self.directories[0]
            # print self.root
            # return
        split_dirs = map(lambda d: d.split('/'), self.directories)
        new_root = []
        for level in zip(*split_dirs):
            if not(all(map(lambda d: d == level[0], level))):
                break
            new_root.append(level[0])
        self.root = '/'.join(new_root)

    def file_count(self):
        return len(self.files)

    def blocks(self, dirs):
        """
        determine if this batch of files blocks work on another batch of
        files. This is the case when a subdir
        """
        return any(map(lambda d: d in self.directories, dirs))

def check_if_blocked(batches, root, dirs):
    paths = [os.path.join(root, d) for d in dirs]
    return any(map(lambda b: b.blocks(paths), batches))


def filter_python_files(files):
    """
    given a list of files, extract the python files
    """
    return filter(lambda f: f.split('.')[-1] == 'py', files)


def crawl(path, LIMIT):
    """
    crawl a given file path, from the deepest node up, collecting and
    organizing directories containing python files into `Batches` of less
    than `LIMIT` python files.
    """
    batches = []
    in_a_batch = False

    for root, dirs, files in os.walk(path, topdown=False):
        # skip directories that have no python files
        if len(filter_python_files(files)) < 1:
            continue
        if not in_a_batch:
            current_batch = Batch(root)
            in_a_batch = True
        if check_if_blocked(batches, root, dirs):
            current_batch.blocked = True
        python_files = [os.path.join(root, f) for f in filter_python_files(files)]
        for file_path in python_files:
            current_batch.add(file_path)
        if current_batch.file_count() >= LIMIT:
            batches.append(current_batch)
            in_a_batch = False
    if in_a_batch:
        batches.append(current_batch)
    return batches



def main():
    path = sys.argv[1]
    batches = crawl(path, MAX_FILE_NUMBER)
    for b in batches:
        print "{} :: {} :: {} :: {}".format(b.root, len(b.top_level_directories), b.file_count(), b.blocked)


if __name__ == '__main__':
    main()
