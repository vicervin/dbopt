import psycopg2
import docker
import traceback
from subprocess import Popen, run
from data_gen.data_generator import DataGenerator
import json
import time
import os

DBOPT_PATH = os.path.abspath(os.path.dirname(__file__))

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

class QueryRunner:

    def __init__(self, user='postgres', host='localhost', port=5432, dbname='tpch', password='postgres',
                        benchmark='tpch', scale_factor=1, dockerized = False, results_dir=f'{DBOPT_PATH}/results'):
        self.host = host
        if dockerized:
            self.host = self.container_name
        self.user = user
        self.password = password
        self.port = port
        self.dbname = dbname
        self.benchmark = benchmark
        self.scale_factor = scale_factor
        self.container_name = f'{benchmark}_{scale_factor}'
        self.dockerized = dockerized
        self.results_dir = results_dir

        

    def run_tpch(self, confDict):
        try:
            self.stop_postgres()
            pg_container = self.start_postgres()
            self.set_config(confDict)
            times = self.run_queries()
            self.save_run(confDict, times, self.results_dir)
            self.stop_postgres()
            return times
        except Exception as e:
            print('runTpch failed due to an Exception:',e)
            traceback.print_exc()
            self.stop_postgres()
            print("Tpch Run has failed and No Query Times returned")
            return None



    def stop_postgres(self):
        docker_client = docker.from_env()  
        running_pgs = docker_client.containers.list(all=True,filters={'ancestor':'postgres'})
        #running_pgs_ = docker_client.containers.list(all=True,filters={'ancestor':f'{BENCHMARK}:{str(scale_factor)}'})
        #running_pgs.extend(x for x in running_pgs_ if x not in running_pgs)
        if running_pgs != []:
            for pg_container in running_pgs:
                pg_container.stop()
                pg_container.remove()

    def wait_for_pg_to_start(self, db):
        for i in range(1,100):
            try:
                with psycopg2.connect(f'host={self.host} port={self.port} dbname={db} user={self.user} password={self.password}') as cnxn:
                    return
            except psycopg2.Error as e:
                time.sleep(6)
        raise Exception("Database takes more than 10 minutes to start")

    def start_postgres(self):
        try:
            docker_client = docker.from_env()
            PGDATA = "postgres/data"
            if self.dockerized:
                current_pg = docker_client.containers.run(
                    image=f'{self.benchmark}:{str(self.scale_factor)}',
                    detach=True,
                    environment={"PGDATA": PGDATA },
                    ports={5432: self.port},
                    name=self.container_name,
                    links={'dbopt_py_1':'dbopt'},
                    network='dbopt_default',
                    #volumes={'postgresql.conf': {'bind': 'postgresql/data/postgresql.conf', 'mode': 'rw'}},
                    shm_size='3G')
            else:
                current_pg = docker_client.containers.run(
                    image=f'{self.benchmark}:{str(self.scale_factor)}',
                    detach=True,
                    environment={"PGDATA": PGDATA },
                    ports={5432: self.port},
                    name=self.container_name,
                    shm_size='3G')
            self.wait_for_pg_to_start("postgres")
            # TODO: Clear caches
            return current_pg

        except docker.errors.APIError as e :
            print(e)

    def check_config(self,confDict):
        failed_config = []
        for param, val in confDict.items():
            result = self.pg_cmd(f"SHOW {param};")
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

    def check_data(self):
        print(f'Checking Data Consistency for tpch:{self.scale_factor}')
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
                val = val * self.scale_factor
            result = self.pg_cmd(f"SELECT count(*) FROM {param};")
            if result == None:
                print(f'!!! Data Mismatch : Expected table {param} is missing!!!') 
                return False
            current_val = result[0][0]
            if (abs(current_val - val) / val) > 0.01 :
                print(f'!!! Data Mismatch : Expected no of rows for table {param} is "{val}" current value is {current_val}') 
                return False
        print(f'Data is Consistent for tpch:{self.scale_factor}')    
        return True

    def pg_cmd(self, cmd, autocommit = False, timeout=0):
        try:
            with psycopg2.connect(f'host={self.host} port={self.port} dbname={self.dbname} user={self.user} password={self.password}') as cnxn:
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

    def set_config(self, confDict):
        for param in confDict:
            self.pg_cmd(f"ALTER SYSTEM SET {param} = '{confDict[param]}';", autocommit=True)
        self.pg_cmd(f"SELECT pg_reload_conf();")
        self.check_config(confDict)            

    def write_config_file(self, fn, confDict):
        with open(fn, 'w') as fp :
            for param, val in confDict.items():
                fp.writelines(f'{param} = {val}\n')

    def run_queries(self):
        times = {}
        count = 0
        for query in QUERIES:
            start = time.time()
            self.pg_cmd(open(f"{DBOPT_PATH}/queries/{query}.sql", "r").read())
            times[query] = time.time() - start
            #print(str(count+1) + " : Query "+ str(QUERIES[count]) + "| time : "+ str(time.time() - start) )
            count += 1
        times['Total'] = sum(times.values())
        print("Total Query time : "+ str(times['Total']))
        return times

    def save_run(self, confDict, times, results_dir):
        dumpDict = {"workload":self.container_name, "input": confDict, "output": times }
        filename = f'{round(time.time())}_{self.benchmark}_{str(self.scale_factor)}'
        with open(f'{DBOPT_PATH}/results/{results_dir}/{filename}.json', 'w') as fp:
            json.dump(dumpDict, fp)

    def build_image(self):
        image_name = f'{self.benchmark}:{str(self.scale_factor)}'
        PGDATA = "postgres/data"
        self.stop_postgres()
        try:
            client = docker.from_env(timeout=600)
            if client.images.list(name=image_name):
                if self.dockerized:
                    container = client.containers.run(
                    image=image_name, detach=True,
                    environment={"PGDATA": PGDATA }, ports={5432: self.port},
                    links={'dbopt_py_1':'dbopt'},  network='dbopt_default',
                    name=self.container_name)
                else:
                    container = client.containers.run(
                    image=image_name,  detach=True,
                    environment={"PGDATA": PGDATA}, ports={5432: self.port},
                    name=self.container_name)
                
                self.wait_for_pg_to_start('postgres')
                if self.check_data():
                    print(f"Machine already has a clean image {image_name}")
                    self.stop_postgres()
                    return True
                else :
                    print(f"Deleting corrupted image {image_name}")
                    self.stop_postgres()
                    client.images.remove(image=image_name)
                    print("Building a new image")
            
            if self.dockerized:
                container = client.containers.run(
                    image='postgres:latest', detach=True, ports={5432: self.port},
                    network='dbopt_default',
                    name=self.container_name)
            else:
                container = client.containers.run(
                    image='postgres:latest', detach=True,
                    ports={5432: self.port}, name=self.container_name)

            self.wait_for_pg_to_start('postgres')
            DataGenerator(
                user=self.user, password=self.password, 
                dbname=self.dbname, host=self.host, port=self.port
                ).run(scale_factor)

            if not self.check_data(scale_factor):
                print(f"Building of image {image_name} failed due to inconsistent data")
                return False
            result = client.containers.get(self.container_name).exec_run(cmd="mkdir /postgres")
            if result.exit_code == 1 :
                print(f"Error in Commiting container to image, {result.output}")
            result = client.containers.get(self.container_name).exec_run(cmd="cp -r /var/lib/postgresql/data/ /postgres/")
            if result.exit_code == 1 :
                print(f"Error in Commiting container to image, {result.output}")
            container.commit(repository=self.benchmark, tag=scale_factor)
            self.stop_postgres()
            print(f"Building of image {image_name} successfully complete")
            return True
                
        except docker.errors.APIError as e :
            print(e)

#run_tpch(confDict,1)
#build_image(10)
#print(str(check_data(10)))