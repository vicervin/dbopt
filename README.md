# dbOpt 
This is a tool used for database configurations tuning currently only configured to run TPC-H workloads on PostrgreSQL.

## Requirements
- Setup python3.6 environment from requirements.txt
- Have docker installed on your machine
- Only tested on Linux but can be ran in docker too

## Execution
- Locally running it : python smac_runQueries.py --sclale_factor [int] --iterations [int]
- With a mounted volume on docker checkout the branch run-db-from-data-folder:
    + Here the parameters are set from the scripts : smac_runQueries for scale and iterations and runTpch for the Mounted volume path
    + On the Kubernetes cluster you need to manually create your own pod for this code, either by pulling from registry.hub.docker.com/vicervin/dbopt:1 (you might need to pull to update repo) or by building your own image using the Dockerfile and uploading it using 'kompose'. You also need to launch the corresponding docker deployment (you should use the yaml file from this repo). Finally you need to add your kubernetes .config file to the pod with the code and from this pod you can run the tool.
    + python smac_runQueries.py --sclale_factor [int] --iterations [int] --on_cluster --label [str] (for multiple runs)
