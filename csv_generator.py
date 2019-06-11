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
def csv_generate(mydir):
    #mydir = os.getcwd()+ '/'+ mydir
    print(mydir)
    onlyfiles = [f for f in os.listdir(mydir) if isfile(join(mydir, f)) and f.endswith('.json')]
    df = pd.DataFrame()
    for json in onlyfiles:
        df = df.append(crunch_json(f'{mydir}/{json}'),ignore_index=True)
    #print(df)
    #df.columns=df.columns.str.strip()
    df = df.sort_values('timestamp').reset_index(drop=True)
    df.to_csv(f'{mydir}/complete.csv')
    

csv_generate('../tpch_30runs')