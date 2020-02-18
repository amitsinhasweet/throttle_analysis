from utility import queries
from utility.logging import *
from app import pre_process

query_run = 0  # 0 for no, 1 for yes
pre_process_run = 1  # 0 for no, 1 for yes

if (query_run == 1):
    queries.run_all()
    logger.info('Queries complete')
if (pre_process_run == 1):
    pre_process.run_all()
    logger.info('pre-processing complete')