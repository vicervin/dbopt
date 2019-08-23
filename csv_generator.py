import pandas as pd
import json

import os
from os.path import isfile, join

def crunch_json(fn):
    jsonRun = json.loads(open(fn).read())
    dictRun = {}
    dictRun.update({ f'conf_{k}': jsonRun['input'][k] for k in jsonRun['input']})
    dictRun.update({ f'queryTime_{k}': jsonRun['output'][k] for k in jsonRun['output']})
    fn = fn.split('/')[-1]
    dictRun.update({ f'timestamp': fn.split('_')[0]})
    dictRun.update({ f'benchmark': fn.split('_')[1]})
    dictRun.update({ f'scale_factor': fn.split('_')[2].split('.')[0]})
    return dictRun


#print(crunch_json('../results/1560200984_tpch_30.json'))
def csv_generate(results_dir, csv_name='run_summary'):
    #mydir = os.getcwd()+ '/'+ mydir
    print(results_dir)
    onlyfiles = [f for f in os.listdir(results_dir) if isfile(join(results_dir, f)) and f.endswith('.json')]
    df = pd.DataFrame()
    for json in onlyfiles:
        df = df.append(crunch_json(f'{results_dir}/{json}'),ignore_index=True)
    #print(df)
    #df.columns=df.columns.str.strip()
    df = df.sort_values('timestamp').reset_index(drop=True)
    df.to_csv(f'{results_dir}/{csv_name}.csv')
    
#remote_results = ['localdesktop_10g_25_3', 'localdesktop_5g_25_3', 'server_10g_25_3', 'server_15g_10_3', 'serverdf_10g_25_3', 'serverdf_15g_100_1', 'serverdf_15g_30_3', 'server_6g_5_3', 'local1_5g_10_1', 'local2_5g_10_1', 'local3_5g_10_1']
def create_csv_summary(results_dir, runs_list):
    df = pd.DataFrame()
    for run in runs_list:
        if not os.path.exists(f'{results_dir}/{run}.csv'):
            csv_generate(f'{results_dir}/{run}', run)
        df_ = pd.read_csv(f'{results_dir}/{run}.csv')
        df_ = df_.rename(columns={'Unnamed: 0':'iteration'})
        df_['Experiment_name'] = run
        df = df.append(df_, ignore_index=True)
        reruns = run.split('_')[-1]
        if reruns.isdigit() and int(reruns) > 1:
            df_avg_ = df_
            df_avg_['Experiment_name'] = run+'_avg_duplicated'
            df_avg = pd.DataFrame()
            reruns = int(reruns)
            func_evals = int(run.split('_')[-2])
            for idx,row_index in enumerate(range(0,func_evals*reruns,reruns)):
                row_ids = [i for i in range(row_index,row_index+reruns)]
                avg_time = df_.loc[row_ids, 'queryTime_Total'].mean()
                df_avg_.loc[row_ids,'queryTime_Total'] = avg_time
                df_avg = df_avg.append(df_avg_.loc[row_index], ignore_index=True)
                df_avg.loc[idx,'iteration'] = idx
            df_avg['Experiment_name'] = run+'_avg_singled'
        df = df.append(df_avg_, ignore_index=True)
        df = df.append(df_avg, ignore_index=True)          
    df.to_csv(f'{results_dir}/summary.csv')

#create_csv_summary('results', remote_results)

#csv_generate('../tpch_30runs')