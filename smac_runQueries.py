"""
An example for the usage of SMAC within Python.
We optimize a simple SVM on the IRIS-benchmark.
Note: SMAC-documentation uses linenumbers to generate docs from this file.
"""

import logging
import time
import argparse
import pandas as pd
import numpy as np
from os import mkdir
from sklearn import svm, datasets
from sklearn.model_selection import cross_val_score

# Import ConfigSpace and different types of parameters
from smac.configspace import ConfigurationSpace
from ConfigSpace.hyperparameters import CategoricalHyperparameter, \
    UniformFloatHyperparameter, UniformIntegerHyperparameter
from ConfigSpace.conditions import InCondition

# Import SMAC-utilities
from smac.tae.execute_func import ExecuteTAFuncDict
from smac.scenario.scenario import Scenario
from smac.facade.smac_facade import SMAC

from runTpch import QueryRunner
import csv_generator
from runTpch import DBOPT_PATH

# We load the iris-dataset (a widely used benchmark)
iris = datasets.load_iris()
#df_results = pd.DataFrame()



class SmacRunner:

    def __init__(self, scale_factor=1, iterations=3, reruns=1, dockerized=False, results_dir=None, on_cluster=False,
                label=None, seed=42, parallel=False):
        self.scale_factor = scale_factor
        self.iterations = iterations
        self.reruns = reruns
        self.dockerized = dockerized
        self.on_cluster = on_cluster
        self.parallel = parallel
        self.label= label
        self.seed = seed
        if results_dir is None:
            self.results_dir = f"{time.strftime('%Y%m%d%H%M%s')}_{scale_factor}g_{iterations}_{reruns}"


    
    def svm_from_cfg(self, cfg):
        """ Creates a SVM based on a configuration and evaluates it on the
        iris-dataset using cross-validation.
        Parameters:
        -----------
        cfg: Configuration (ConfigSpace.ConfigurationSpace.Configuration)
            Configuration containing the parameters.
            Configurations are indexable!
        Returns:
        --------
        A crossvalidated mean score for the svm on the loaded data-set.
        """
        # For deactivated parameters, the configuration stores None-values.
        # This is not accepted by the SVM, so we remove them.
        cfg = {k : cfg[k] for k in cfg if cfg[k]}
        # We translate boolean values:
        cfg["shrinking"] = True if cfg["shrinking"] == "true" else False
        # And for gamma, we set it to a fixed value or to "auto" (if used)
        if "gamma" in cfg:
            cfg["gamma"] = cfg["gamma_value"] if cfg["gamma"] == "value" else "auto"
            cfg.pop("gamma_value", None)  # Remove "gamma_value"

        clf = svm.SVC(**cfg, random_state=42)

        scores = cross_val_score(clf, iris.data, iris.target, cv=5)
        return 1-np.mean(scores)  # Minimize!

    def benchmark_from_cfg(self, cfg):
        cfg = {k: cfg[k] for k in cfg if cfg[k]}

        for parameter in ['work_mem', 'temp_buffers', 'maintenance_work_mem']:
            cfg[parameter] = str(cfg[parameter])+"MB"

        if 'effective_cache_size' in cfg:
            cfg['effective_cache_size'] = str(cfg['effective_cache_size'])+"GB"
        #cfg.pop('shared_buffers',None)
        query_runner = QueryRunner(
            scale_factor=self.scale_factor, 
            dockerized=self.dockerized,
            results_dir=self.results_dir,
            label=self.label,
            on_cluster=self.on_cluster)
        score = 0
        for i in range(self.reruns):
            times = query_runner.run_tpch(cfg)
            score = score + times['Total']
        score = score / self.reruns

        #dict_results = {}
        #dict_results.update({ f'conf_{k}': cfg[k] for k in cfg})
        #dict_results.update({ f'queryTimes_{k}': times[k] for k in times})
        #global df_results
        #df_results = df_results.append(dict_results,ignore_index=True)
        #return times['Total']
        return score

    def run(self):
        if not self.on_cluster:
            QueryRunner(scale_factor=self.scale_factor, dockerized=self.dockerized).build_image()
        mkdir(f'{DBOPT_PATH}/results/{self.results_dir}')
        if self.parallel:
            parallel_dir = f'{DBOPT_PATH}/results/{self.results_dir}/parallel'
            mkdir(f'{DBOPT_PATH}/results/{self.results_dir}/parallel')
        
        #logger = logging.getLogger("SVMExample")
        logging.basicConfig(level=logging.INFO)  # logging.DEBUG for debug output

        # Build Configuration Space which defines all parameters and their ranges
        cs = ConfigurationSpace()

        # We define a few possible types of SVM-kernels and add them as "kernel" to our cs
        #kernel = CategoricalHyperparameter("kernel", ["linear", "rbf", "poly", "sigmoid"], default_value="poly")
        #cs.add_hyperparameter(kernel)

        # There are some hyperparameters shared by all kernels
        work_mem = UniformIntegerHyperparameter("work_mem", 1, 4000, default_value=4)
        temp_buffers = UniformIntegerHyperparameter("temp_buffers", 1, 1000, default_value=8)
        shared_buffers = UniformIntegerHyperparameter("shared_buffers", 1, 8000, default_value=128)
        effective_cache_size = UniformIntegerHyperparameter("effective_cache_size", 1, 8, default_value=4)
        maintenance_work_mem = UniformIntegerHyperparameter("maintenance_work_mem", 1, 8000, default_value=64)

        max_parallel_workers = UniformIntegerHyperparameter("max_parallel_workers", 1, 8, default_value=8)
        effective_io_concurrency = UniformIntegerHyperparameter("effective_io_concurrency", 1, 254, default_value=1)
        parallel_setup_cost = UniformIntegerHyperparameter("parallel_setup_cost", 1, 10000, default_value=1000)
        seq_page_cost = UniformIntegerHyperparameter("seq_page_cost", 1, 16, default_value=1)
        random_page_cost = UniformIntegerHyperparameter("random_page_cost", 1, 16, default_value=4)

        #shrinking = CategoricalHyperparameter("shrinking", ["true", "false"], default_value="true")
        cs.add_hyperparameters([work_mem, temp_buffers, effective_cache_size, maintenance_work_mem,
                                max_parallel_workers, effective_io_concurrency, seq_page_cost, random_page_cost])

        # # Others are kernel-specific, so we can add conditions to limit the searchspace
        # degree = UniformIntegerHyperparameter("degree", 1, 5, default_value=3)     # Only used by kernel poly
        # coef0 = UniformFloatHyperparameter("coef0", 0.0, 10.0, default_value=0.0)  # poly, sigmoid
        # cs.add_hyperparameters([degree, coef0])
        # use_degree = InCondition(child=degree, parent=kernel, values=["poly"])
        # use_coef0 = InCondition(child=coef0, parent=kernel, values=["poly", "sigmoid"])
        # cs.add_conditions([use_degree, use_coef0])
        #
        # # This also works for parameters that are a mix of categorical and values from a range of numbers
        # # For example, gamma can be either "auto" or a fixed float
        # gamma = CategoricalHyperparameter("gamma", ["auto", "value"], default_value="auto")  # only rbf, poly, sigmoid
        # gamma_value = UniformFloatHyperparameter("gamma_value", 0.0001, 8, default_value=1)
        # cs.add_hyperparameters([gamma, gamma_value])
        # # We only activate gamma_value if gamma is set to "value"
        # cs.add_condition(InCondition(child=gamma_value, parent=gamma, values=["value"]))
        # # And again we can restrict the use of gamma in general to the choice of the kernel
        # cs.add_condition(InCondition(child=gamma, parent=kernel, values=["rbf", "poly", "sigmoid"]))


        # Scenario object
        scenario = Scenario({"run_obj": "quality",   # we optimize quality (alternatively runtime)
                            "runcount-limit": self.iterations,  # maximum function evaluations
                            "cs": cs,               # configuration space
                            "deterministic": "true"
                            })
        if self.parallel:
            scenario = Scenario({"run_obj": "quality",  "runcount-limit": self.iterations, "cs": cs,
                            "deterministic": "true", "shared_model":True, "input_psmac_dirs": parallel_dir
                            }) 
        # Example call of the function
        # It returns: Status, Cost, Runtime, Additional Infos
        def_value = self.benchmark_from_cfg(cs.get_default_configuration())
        print("Default Value: %.2f" % (def_value))

        # Optimize, using a SMAC-object
        print("Optimizing! Depending on your machine, this might take a few minutes.")
        smac = SMAC(scenario=scenario, rng=np.random.RandomState(self.seed),
                tae_runner=self.benchmark_from_cfg)

        incumbent = smac.optimize()

        inc_value = self.benchmark_from_cfg(incumbent)

        print("Optimized Value: %.2f" % (inc_value))


        # We can also validate our results (though this makes a lot more sense with instances)
        smac.validate(config_mode='inc',      # We can choose which configurations to evaluate
                    #instance_mode='train+test',  # Defines what instances to validate
                    repetitions=100,        # Ignored, unless you set "deterministic" to "false" in line 95
                    n_jobs=1) # How many cores to use in parallel for optimization

        #df_results.to_csv(f'{DBOPT_PATH}/results/{self.results_dir}/{time.time()}_tpch_{scale_factor}.csv')
        csv_generator.csv_generate(f'{DBOPT_PATH}/results/{self.results_dir}', self.results_dir)

if __name__ == '__main__':
    args_to_parse = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)

    args_to_parse.add_argument('--reruns', required=False, type=int, help=(
        'The number of repititions that a single function evaluation would take, where the score is then the average.'
    ))

    args_to_parse.add_argument('--scale_factor', required=True, type=int, help=(
        'The scale factor of the benchmark to be optimized. type = Integer, unit = GB'
    ))

    args_to_parse.add_argument('--iterations', required=True, type=int, help=(
        'The number of function evaluations made until optimization stops'
    ))

    args_to_parse.add_argument('--dockerized', required=False, default=False, action='store_true', help=(
        'If instance is running from inside a docker container'
    ))
    
    args_to_parse.add_argument('--on_cluster', required=False, default=False, action='store_true', help=(
        'If instance is running from inside a Kubernetes cluster'
    ))
    
    args_to_parse.add_argument('--results_dir', required=False, type=str, help=(
        'Path for storing the results of this run'
    ))

    args_to_parse.add_argument('--parallel', required=False, default=False, action='store_true', help=(
        'If instance is running parallel SMAC runs'
    ))

    args_to_parse.add_argument('--label', required=False,default=None, help=(
        'If parallel label for specific db pod'
    ))

    args_to_parse.add_argument('--seed', required=False, default=None, help=(
        'If parallel label for different SMAC seed'
    ))
    
    args = args_to_parse.parse_args()
    if args.parallel:
        if args.label is None or args.seed is None:
            argparse.ArgumentParser().error("Seed and label need to be set for parallel runs")
    SmacRunner(**vars(args)).run()