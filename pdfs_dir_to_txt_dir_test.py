import os
import multiprocessing
import glob
import shutil
import sys


from arxiv_public_data.fulltext import convert_directory_parallel
from arxiv_public_data.s3_bulk_download import _make_pathname
from arxiv_public_data.s3_bulk_download import _call
from arxiv_public_data.config import DIR_FULLTEXT

pdfs_dir = '/home/qmn203/data_testset'

def move_txt(pdfs_dir,dryrun=False):
    globber_text = os.path.join(pdfs_dir, '**/*.txt') # search expression for glob.glob
    txtfiles = glob.glob(globber_text,recursive=True)
    for tf in txtfiles:  # https://github.com/mattbierbaum/arxiv-public-datasets/blob/8b2e49fde5a1be13c777638d5f60cf0f5e0ae759/arxiv_public_data/s3_bulk_download.py#L233 
        print(tf)
        mvfn = _make_pathname(tf)
        print(mvfn)
        dirname = os.path.dirname(mvfn)
        if not os.path.exists(dirname):
            _call('mkdir -p {}'.format(dirname, dryrun))

        if not dryrun:
            shutil.move(tf, mvfn)  # recursive move tf to mvfn

if __name__ == '__main__':
    num_processes  = multiprocessing.cpu_count() if len(sys.argv) <= 1 else int(sys.argv[1])
    print('number of process: ',num_processes)
    print(sys.prefix)
    convert_directory_parallel(pdfs_dir, num_processes)
    #move_txt(pdfs_dir)
