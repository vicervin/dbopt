import psycopg2
import time

server = 'localhost'
database = 'tpch'
username = 'postgres'
password = 'postgres'

#queries = [14,2,9,#20,6,#17,#18,8,#21,13,3,22,16,4,11,15,1,10,19,5,7,12]
queries = [14,2,9,6,8,13,3,22,16,4,11,15,1,10,19,5,7,12]
            
def runQueries(timeout):
            times = []
            count = 0
            for query in queries:
                    try:
                            cnxn = psycopg2.connect(f'host={server} dbname={database} user={username} password={password}')
                            
                            start = time.time()
                            cursor = cnxn.cursor()
                            #cursor.execute(f"SET statement_timeout = '{timeout}s'")
                            cursor.execute(open(f"queries/{query}.sql", "r").read())
                            times.append(time.time() - start)
                            #(number_of_rows,)=cursor.fetchone()
                            print(str(count+1) + " : Query "+ str(queries[count]) + "| time : "+ str(time.time() - start) )#+ "| rows:" + str(number_of_rows))
                            cursor.close()
                    except Exception as e:
                            print(str(count+1) + " : Query "+ str(queries[count]) +" : "+ str(e))
                    count += 1
            print("Total Query time : "+ str(sum(times)))
            return sum(times)

#runQueries(200)

def applyConf(confDict):
        cnxn = psycopg2.connect(f'host={server} dbname={database} user={username} password={password}')                            
        cursor = cnxn.cursor()
                            
        for conf in confDict:
                cursor.execute(f"ALTER SYSTEM SET {conf} = '{confDict[conf]}s'")
        cursor.close()
        return sum(times)
param=8
def checkConfig():
        with psycopg2.connect(f'host={server} dbname={database} user={username} password={password}') as cnxn:
                cnxn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT) 
                with cnxn.cursor() as cursor:
                        cursor.execute(f"ALTER SYSTEM SET max_parallel_workers = '{param}';")            
                with cnxn.cursor() as cursor:
                        cursor.execute(f"SELECT pg_reload_conf();")            

        with psycopg2.connect(f'host={server} dbname={database} user={username} password={password}') as cnxn:
                with cnxn.cursor() as cursor:
                        cursor.execute(f"SHOW max_parallel_workers;")#SELECT pg_reload_conf();")
                        print(f"!!!!!!!!!!!!! !!!!!!!!!!! {str(param)==str(cursor.fetchone()[0])} !!!!!!!!!!!!!!!!!!!!!") 
                

checkConfig()
