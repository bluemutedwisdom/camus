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
    -i --ignore_before_date=<date>      Do not create Hive partitions before a certain start date, default is None
    -v --verbose                        Turn on verbose debug logging.
    -n --dry-run                        Don't actually create any partitions, just output the Hive queries to add partitions.
"""
__author__ = 'Andrew Otto <otto@wikimedia.org>'

from   datetime import datetime
from   docopt   import docopt
import logging

from pykafka import KafkaClient

from util import HiveUtils, HdfsDatasetUtils, diff_datewise, interval_hierarchies

def load_properties(filepath, sep='=', comment_char='#'):
    """
    Read the file passed as parameter as a properties file.
    """
    props = {}
    with open(filepath, "rt") as f:
        for line in f:
            l = line.strip()
            if l and not l.startswith(comment_char):
                key_value = l.split(sep)
                key = key_value[0].strip()
                value = sep.join(key_value[1:]).strip('" \t')
                props[key] = value
    return props

def fetch_kafka_trekkie_topics(kafka_brokers):
    client = KafkaClient(hosts=kafka_brokers)
    return filter((lambda topic: topic.startswith('trekkie')), client.topics.keys())

if __name__ == '__main__':
    arguments = docopt(__doc__)

    datadir                = arguments['<datadir>']
    database               = arguments['--database']
    tables                 = arguments['--tables']
    hive_options           = arguments['--hive-options']
    ignore_before_date     = arguments['--ignore_before_date']
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
    else:
        properties = load_properties('/u/apps/camus/shared/camus.properties')
        tables = fetch_kafka_trekkie_topics(properties['kafka.brokers'])

    hive = HiveUtils(database, hive_options)
    for table in tables:
        hdfs_location = table.replace('_', '.')
        table = table.replace('.', '_').replace('-', '_')
        if not hive.table_exists(table):
            if dry_run:
                logging.info(hive.create_table_ddl(table, hdfs_location))
            else:
                hive.table_create(table, hdfs_location)

        if dry_run:
            logging.info(str(hive.get_missing_partitions_ddl(table)))
        else:
            hive.add_missing_partitions(table)

