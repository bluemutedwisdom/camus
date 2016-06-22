#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

""" Automatically adds Hive partitions based on time bucketed HDFS directory hierarchy.
Usage: hive-partitioner [options] <datadir>

Options:
    -h --help                           Show this help message and exit.
    -D --database=<dbname>              Hive database name.  [default: default]
    -t --tables=<table1[,table2...]>    Tables to create partitions for.  If not specified,
                                        all directories found in <datadir> will be partitioned.
    -o --hive-options=<options>         Any valid Hive CLI options you want to pass to Hive commands.
                                        Example: '--auxpath /path/to/hive-serdes-1.0-SNAPSHOT.jar'
    -v --verbose                        Turn on verbose debug logging.
    -n --dry-run                        Don't actually create any partitions, just output the Hive queries to add partitions.
"""
__author__ = 'Andrew Otto <otto@wikimedia.org>'

from   datetime import datetime
from   docopt   import docopt
import logging

from util import HiveUtils, HdfsDatasetUtils, diff_datewise, interval_hierarchies
from hive_trekkie import hive_trekkie_create_table_stmt

if __name__ == '__main__':
    arguments = docopt(__doc__)

    datadir                = arguments['<datadir>']
    database               = arguments['--database']
    tables                 = arguments['--tables']
    hive_options           = arguments['--hive-options']
    verbose                = arguments['--verbose']
    dry_run                = arguments['--dry-run']

    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level,
                        format='%(asctime)s %(levelname)-6s %(message)s',
                        datefmt='%Y-%m-%dT%H:%M:%S')

    if tables:
        tables = tables.split(',')

    hive = HiveUtils(database, hive_options)
    for table in tables:
        table = table.replace('.', '_')
        if not hive.table_exists(table):
            if dry_run:
                logging.info(str(hive_trekkie_create_table_stmt(table)))
            else:
                hive.table_create(table)

        if dry_run:
            logging.info(str(hive.get_missing_partitions_ddl(table)))
        else:
            hive.add_missing_partitions(table)

