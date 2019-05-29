import psycopg2
import docker
from subprocess import Popen, run
import json
import time

server = 'localhost'
database = 'tpch'
username = 'postgres'
password = 'postgres'
BENCHMARK = 'tpch'
SCALE_FACTOR = '10'
CONTAINER_NAME = f'{BENCHMARK}_{SCALE_FACTOR}'
DATA_PATH = "/home/ervin/Documents/Uni/Thesis/data_backups/"+CONTAINER_NAME
#QUERIES = [14,2,9,#20,6,#17,#18,8,#21,13,3,22,16,4,11,15,1,10,19,5,7,12]
QUERIES = [14,2,9,6,8,13,3,22,16,4,11,15,1,10,19,5,7,12]

confDict = {
    "max_parallel_workers" : "8" ,
    "work_mem" : "2000MB" ,
    "max_parallel_workers_per_gather" : "2" ,
    "parallel_setup_cost" : "1000" ,
    "shared_buffers" : "128MB" ,
    "temp_buffers" : "8MB" ,
    "effective_cache_size" : "4GB" ,
    "maintenance_work_mem" : "64MB" ,
    "effective_io_concurrency" : "1" ,
    "max_worker_processes" : "8" ,
    "seq_page_cost" : "1" ,
    "random_page_cost" : "4" 
    }

def run_tpch(confDict):
    #data_path = copy_data_dir(str_schema_scale)
    pg_container = start_postgres()
    pg_cmd("ALTER SYSTEM RESET ALL;", autocommit=True)
    set_config(confDict)
    times = run_queries()
    save_run(confDict, times)
    stop_postgres(pg_container)
    return times

def stop_postgres(container):
    container.stop()
    container.remove()

def wait_for_pg_to_start():
    for i in range(1,10):
        try:
            with psycopg2.connect(f'host={server} dbname={database} user={username} password={password}') as cnxn:
                return
        except psycopg2.Error as e:
            time.sleep(6)
    raise Exception("Database takes more than 60 seconds to start")

def start_postgres():
    try:
        docker_client = docker.from_env()
        running_pgs = docker_client.containers.list(all=True,filters={'ancestor':'postgres'})
        if running_pgs != []:
            for pg_container in running_pgs:
                stop_postgres(pg_container)
        current_pg = docker_client.containers.run(
            image='postgres',
            detach=True,
            ports={5432: 5432},
            name=CONTAINER_NAME,
            volumes={DATA_PATH: {'bind': '/var/lib/postgresql/data', 'mode': 'rw'}})
        wait_for_pg_to_start()
        return current_pg


    except docker.errors.APIError as e :
        print(e)
    # try:        
    #     Popen(['docker','run', '-d', '-p', '5432:5432', '--name', CONTAINER_NAME, 
    #     '-v', f'{DATA_PATH}:/var/lib/postgresql/data', 'postgres'],check=True)
    # except Exception as e:
    #     print(e)
    #     Popen(['docker', 'stop', CONTAINER_NAME])
    #     Popen(['docker', 'rm', CONTAINER_NAME])
    #     Popen(['docker','run', '-d', '-p', '5432:5432', '--name', CONTAINER_NAME, 
    #     '-v', f'{DATA_PATH}:/var/lib/postgresql/data', 'postgres'])

def check_config(confDict):
    failed_config = []
    for param, val in confDict.items():
        current_val = pg_cmd(f"SHOW {param};")[0][0]
        if str(val) != current_val:

            print(f'!!! Config FAILURE : parameter "{param}" failed to set {val}, current value is {current_val}') 
            failed_config.append(param)
    [confDict.pop(param, None) for param in failed_config]
    # if failed_config == [] :
    #    print("Configurationss Successfully set")

def pg_cmd(cmd, autocommit = False):
    try:
        with psycopg2.connect(f'host={server} dbname={database} user={username} password={password}') as cnxn:
            if autocommit:
                cnxn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
            with cnxn.cursor() as cursor:
                cursor.execute(cmd)
                if cursor.description is not None:
                    return cursor.fetchall()
                return None
    except psycopg2.Error as e:
            print(f'Database connection error: {str(e)}')

def set_config(confDict):
    for param in confDict:
        pg_cmd(f"ALTER SYSTEM SET {param} = '{confDict[param]}';", autocommit=True)
    pg_cmd(f"SELECT pg_reload_conf();")
    check_config(confDict)            

def run_queries():
    times = {}
    count = 0
    for query in QUERIES:
        start = time.time()
        pg_cmd(open(f"queries/{query}.sql", "r").read())
        times[query] = time.time() - start
        # print(str(count+1) + " : Query "+ str(QUERIES[count]) + "| time : "+ str(time.time() - start) )
        count += 1
    # print("Total Query time : "+ str(sum(times.values())))
    return times

def save_run(confDict, times):
    dumpDict = {"workload":CONTAINER_NAME, "input": confDict, "output": times }
    with open(f'../results/{time.time()}.json', 'w') as fp:
        json.dump(dumpDict, fp)

#run_tpch(confDict)