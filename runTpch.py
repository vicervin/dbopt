import psycopg2
import docker
from subprocess import Popen, run
from data_gen import data_generator
import json
import time

server = 'localhost'
database = 'tpch'
username = 'postgres'
password = 'postgres'
BENCHMARK = 'tpch'
SCALE_FACTOR = 1
CONTAINER_NAME = f'{BENCHMARK}_{str(SCALE_FACTOR)}'
DATA_PATH = "/home/ervin/Documents/Uni/Thesis/data_backups/"+CONTAINER_NAME
#QUERIES = [14,2,9,#20,6,#17,#18,8,#21,13,3,22,16,4,11,15,1,10,19,5,7,12]
#QUERIES = [14,2,9,6,8,13,3,22,16,4,11,15,1,10,19,5,7,12]

QUERIES = [14,2,9,"20_rewritten",6,"17_rewritten",8,13,3,22,16,4,11,15,1,10,19,5,7,12]
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

def run_tpch(confDict, scale_factor):
    try:
        #data_path = copy_data_dir(str_schema_scale)
        stop_postgres()
        pg_container = start_postgres(scale_factor)
        set_config(confDict)
        times = run_queries()
        save_run(confDict, times, scale_factor)
        stop_postgres()
        return times
    except Exception as e:
        print(e)
        stop_postgres()
        print("Tpch Run has failed and No Query Times returned")
        return None



def stop_postgres():
    docker_client = docker.from_env()  
    running_pgs = docker_client.containers.list(all=True,filters={'ancestor':'postgres'})
    #running_pgs_ = docker_client.containers.list(all=True,filters={'ancestor':f'{BENCHMARK}:{str(scale_factor)}'})
    #running_pgs.extend(x for x in running_pgs_ if x not in running_pgs)
    if running_pgs != []:
        for pg_container in running_pgs:
            pg_container.stop()
            pg_container.remove()

def wait_for_pg_to_start(db=database):
    for i in range(1,100):
        try:
            with psycopg2.connect(f'host={server} dbname={db} user={username} password={password}') as cnxn:
                return
        except psycopg2.Error as e:
            time.sleep(6)
    raise Exception("Database takes more than 10 minutes to start")

def start_postgres(scale_factor):
    try:
        docker_client = docker.from_env()
        current_pg = docker_client.containers.run(
            image=f'{BENCHMARK}:{str(scale_factor)}',
            detach=True,
            environment={"PGDATA": "postgres/data"},
            ports={5432: 5432},
            name=CONTAINER_NAME,
            links={'dbopt_py_1':'dbopt'},
            network='dbopt_default',
            #volumes={'postgresql.conf': {'bind': 'postgresql/data/postgresql.conf', 'mode': 'rw'}},
            shm_size='3G')
        wait_for_pg_to_start("postgres")
        # TODO: Clear caches
        return current_pg


    except docker.errors.APIError as e :
        print(e)

def check_config(confDict):
    failed_config = []
    for param, val in confDict.items():
        result = pg_cmd(f"SHOW {param};")
        if result == None:
            print(f'!!! Config FAILURE : parameter "{param}" failed to set {val}, wrong parameter name or error in SQL command') 
            failed_config.append(param)
            continue
        current_val = result[0][0]
        if str(val) != current_val:

            print(f'!!! Config FAILURE : parameter "{param}" failed to set {val}, current value is {current_val}') 
            failed_config.append(param)
    [confDict.pop(param, None) for param in failed_config]
    # if failed_config == [] :
    #    print("Configurationss Successfully set")

def check_data(scale_factor):
    print(f'Checking Data Consistency for tpch:{scale_factor}')
    rowsDict = { "nation": 25,
    "customer":  150000,
    "part":  200000,
    "partsupp":  800000,
    "orders":  1500000,
    "region":  5,
    "supplier":  10000,
    "lineitem":  6001215}
    for param, val in rowsDict.items():
        if param not in ['region', 'nation']:
            val = val * scale_factor
        result = pg_cmd(f"SELECT count(*) FROM {param};")
        if result == None:
            print(f'!!! Data Mismatch : Expected table {param} is missing!!!') 
            return False
        current_val = result[0][0]
        if (abs(current_val - val) / val) > 0.01 :
            print(f'!!! Data Mismatch : Expected no of rows for table {param} is "{val}" current value is {current_val}') 
            return False
    print(f'Data is Consistent for tpch:{scale_factor}')    
    return True

def pg_cmd(cmd, autocommit = False, timeout=0):
    try:
        with psycopg2.connect(f'host={server} dbname={database} user={username} password={password}') as cnxn:
            if autocommit:
                cnxn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
            with cnxn.cursor() as cursor:
                #if timeout > 0 :
                #    cursor.execute(f'SET STATEMENT_TIMEOUT={self.timeout * 1000}')
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

def write_config_file(fn, confDict):
    with open(fn, 'w') as fp :
        for param, val in confDict.items():
            fp.writelines(f'{param} = {val}\n')

def run_queries():
    times = {}
    count = 0
    for query in QUERIES:
        start = time.time()
        pg_cmd(open(f"queries/{query}.sql", "r").read())
        times[query] = time.time() - start
        #print(str(count+1) + " : Query "+ str(QUERIES[count]) + "| time : "+ str(time.time() - start) )
        count += 1
    times['Total'] = sum(times.values())
    print("Total Query time : "+ str(times['Total']))
    return times

def save_run(confDict, times, scale_factor):
    dumpDict = {"workload":CONTAINER_NAME, "input": confDict, "output": times }
    with open(f'../results/{round(time.time())}_{BENCHMARK}_{str(scale_factor)}.json', 'w') as fp:
        json.dump(dumpDict, fp)

def build_image(scale_factor):
    image_name = f'{BENCHMARK}:{str(scale_factor)}'
    try:
        client = docker.from_env(timeout=600)
        if client.images.list(name=image_name):
            container = client.containers.run(
                image=image_name,
                detach=True,
                environment={"PGDATA": "postgres/data"},
                ports={5432: 5432},
                links={'dbopt_py_1':'dbopt'},
                network='dbopt_default',
                name=CONTAINER_NAME)
            wait_for_pg_to_start('postgres')
            if check_data(scale_factor):
                print(f"Machine already has a clean image {image_name}")
                stop_postgres()
                return True
            else :
                print(f"Deleting corrupted image {image_name}")
                stop_postgres()
                client.images.remove(image=image_name)
                print("Building a new image")
    
        container = client.containers.run(
                        image='postgres:latest',
                        detach=True,
                        ports={5432: 5432},
                        network='dbopt_default',
                        name=CONTAINER_NAME)
        wait_for_pg_to_start('postgres')
        data_generator.run(scale_factor)
        if not check_data(scale_factor):
            print(f"Building of image {image_name} failed due to inconsistent data")
            return False
        result = client.containers.get(CONTAINER_NAME).exec_run(cmd="mkdir /postgres")
        if result.exit_code == 1 :
            print(f"Error in Commiting container to image, {result.output}")
        result = client.containers.get(CONTAINER_NAME).exec_run(cmd="cp -r /var/lib/postgresql/data/ /postgres/")
        if result.exit_code == 1 :
            print(f"Error in Commiting container to image, {result.output}")
        container.commit(repository=BENCHMARK, tag=scale_factor)
        stop_postgres()
        print(f"Building of image {image_name} successfully complete")
        return True
            
    except docker.errors.APIError as e :
        print(e)

#run_tpch(confDict,1)
#build_image(10)
#print(str(check_data(10)))