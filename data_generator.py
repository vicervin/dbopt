#!/usr/bin/env python3

import math
from subprocess import Popen, PIPE
from concurrent.futures import ProcessPoolExecutor, as_completed
import os

FULL_PATH = os.path.dirname(os.path.abspath(__file__))

USER = "postgres"
HOST = "localhost"
PORT = 5432
DBNAME = "tpch"
dbgen_workdir = FULL_PATH
dbgen_executable = "dbgen"
SCHEMA_PATH = f'{FULL_PATH}/schema.sql'
INDEX_PATH = f'{FULL_PATH}/tpch_index.sql'

TPCH_TABLE_MAP = {
    'customer': 'c',
    'lineitem': 'L',
    'nation':   'n',
    'orders':   'O',
    'part':     'P',
    'partsupp': 'S',
    'region':   'r',
    'supplier': 's'
}

def generate_data(scale_factor):

        chunks = math.ceil(scale_factor / 2)

        # max_workers is not given, so it defaults to the number of processors on the machine
        with ProcessPoolExecutor() as executor:
            futures = {}

            for table_name, table_code in TPCH_TABLE_MAP.items():
                # These two are too small and dbgen seems to have a bug.
                if table_name in ['region', 'nation']:
                    future = executor.submit(ingest_table_chunk,
                                             USER, HOST, PORT, DBNAME,
                                             dbgen_workdir, dbgen_executable,
                                             scale_factor,
                                             table_name, table_code)
                    futures[future] = f'{table_name}'
                else:
                    for build_steps in range(1, chunks + 1):
                        future = executor.submit(ingest_table_chunk,
                                                 USER, HOST, PORT, DBNAME,
                                                 dbgen_workdir, dbgen_executable,
                                                 scale_factor,
                                                 table_name, table_code, chunks, build_steps)
                        futures[future] = f'{table_name}_{build_steps}'

            for future in as_completed(futures):
                identifier = futures[future]
                print(f'{identifier}. done: {future.done()}. result: {future.result()}')

                if future.exception():
                    raise AirflowException(f'DBGen failed in {identifier} '
                                           f'with the exception {future.exception()}')

def ingest_table_chunk(user, host, port,
                       dbname, dbgen_workdir, dbgen_executable,
                       scale_factor,
                       table_name, table_code, chunks=1, build_steps=1):
    cmd = f'./{dbgen_executable} -fo -s {scale_factor} -T {table_code}'

    if chunks > 1:
        cmd += f' -C {chunks} -S {build_steps}'

    sed_cmd = r'sed s/\|$//'

    ingest_cmd = (f'psql -U {user} -h {host} -p {port} '
                  f'-d {dbname} -w -c "COPY '
                  f'{table_name} FROM STDIN WITH DELIMITER \'|\'"')

    print(f'Running {cmd} | {sed_cmd} | {ingest_cmd}')
    proc = Popen(f'{cmd} | {sed_cmd} | {ingest_cmd}',
                 shell=True,
                 stdout=PIPE, stderr=PIPE)


    output, error = proc.communicate()

    if proc.returncode != 0:
        raise Exception(f'DBGen failed in {table_name}_{build_steps} '
                               f'with the exception {error}')

    return output

def run_psql_cmd(psql_cmd, db=True, file_=True):

    plain_cmd = psql_cmd
    if file_:
        psql_cmd = '-f ' + psql_cmd
    else :
        psql_cmd = '-c ' + psql_cmd
    
    if db:
        psql_cmd = f"-d {DBNAME} {psql_cmd}"

    proc = Popen(f'psql -h localhost -U postgres {psql_cmd}',
                    shell=True,
                    stdout=PIPE, stderr=PIPE)

    output, error = proc.communicate()
    if proc.returncode != 0:
            raise Exception(f'command "{plain_cmd}" failed, error : {error}')

def run(scale_factor):
    run_psql_cmd(f'"CREATE DATABASE {DBNAME};"', db=False, file_=False )
    run_psql_cmd(SCHEMA_PATH)
    generate_data(scale_factor)
    run_psql_cmd(INDEX_PATH)
