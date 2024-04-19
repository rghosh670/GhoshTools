"""Main module."""

import ast
import tempfile
from pprint import pprint
import dill as pickle
import pandas as pd
import time
import inspect
import os
from icecream import ic
import subprocess
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import logging
import re
import shutil
import glob

from ghoshtools import GT_GLOBALS, utils
from importlib import resources

logger = logging.getLogger('ghoshtools')

def safe_clear_directory(directory_path):
    # List all files and directories in the directory
    items = glob.glob(os.path.join(directory_path, '*'))

    for item in items:
        try:
            if os.path.isdir(item):
                shutil.rmtree(item)  # Remove directory and all its contents
            else:
                os.remove(item)  # Remove a file
        except Exception as e:
            logger.warn(f"Error removing {item}: {e}")

def clear_working_dir(work_dir_path):
    items = os.listdir(work_dir_path)

    # Filter out 'conda' from the list of items to potentially remove
    items_to_remove = [item for item in items if item != 'conda']

    # Proceed only if there are items other than 'conda'
    if items_to_remove:
        for item in items_to_remove:
            item_path = os.path.join(work_dir_path, item)
            # Determine if it's a file or directory and construct the appropriate rm command
            if os.path.isdir(item_path):
                rm_cmd = f"rm -rf '{item_path}'"
            else:
                rm_cmd = f"rm -f '{item_path}'"

            # Execute the rm command
            try:
                result = subprocess.run(rm_cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                logger.info(f"Removed: {item_path}")
            except subprocess.CalledProcessError as e:
                logger.error(f"Error removing {item_path}: {e}")

        logger.info("Work dir cleared successfully")
    else:
        logger.info("No items to remove, work dir already clean or only contains 'conda'")
    return

def convert_nf_output_to_list_of_pickle_files(text):
    # Regex pattern for matching '.pkl' files
    pattern = r'\b\S+\.pkl\b'
    
    # Use a set to collect unique matches
    valid_paths_set = set(re.findall(pattern, text))
    
    # Convert the set to a list before returning
    return list(valid_paths_set)

def load_pickle_obj_from_file(pickled_obj_file_path):
    with open(pickled_obj_file_path, 'rb') as f:
        my_obj = pickle.load(f)
    return my_obj

def pickle_dump_iterable(element, temp_iterable_dir_path):
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=temp_iterable_dir_path, suffix=".pkl") as obj_file_path:
        pickle.dump(element, obj_file_path)
        obj_file_path.flush()
    return obj_file_path.name

def append_partition_to_log_filename(log_file_path, partition):
    # Split the file path into directory and file name
    dir_name, file_name = os.path.split(log_file_path)
    
    # Split the file name into name and extension
    file_root, file_ext = os.path.splitext(file_name)
    
    # Append the partition value to the file root
    new_file_name = f"{file_root}_{partition}{file_ext}"
    
    # Reassemble the new file path
    new_log_file_path = os.path.join(dir_name, new_file_name)
    
    return new_log_file_path

def safe_clear_work_dir():
    safe_clear_directory(os.path.join(GT_GLOBALS.SCRATCH_DIR, '../work'))
    logger.info(f"Cleared element pickle files from previous runs")
    return

def split_series_by_weight(series, partition, weighting_dict):
    """
    Splits a Pandas Series into n inner lists based on a weighting dictionary.
    
    :param series: Pandas Series to split.
    :param n: Number of inner lists to split the series into.
    :param weighting_dict: Dictionary with keys representing the index of the inner list (0 to n-1)
                           and values representing the weight of each list.
    :return: A list of lists, where each inner list is a part of the original series split according to the specified weights.
    """
    # Ensure the weights sum to the total number of parts (n)
    total_weight = sum(weighting_dict.values())
    total_length = len(series)
    
    # Calculate the total number of elements that a single weight unit represents
    elements_per_weight = total_length / total_weight
    
    # Initialize an empty list to hold the split series
    split_lists = []
    
    # Calculate the starting index for the first split
    start_index = 0
    
    for _, part in enumerate(partition):
        weight = weighting_dict.get(part, 0)  # Default to 0 if not specified
        # Calculate the number of elements for this part
        num_elements = int(round(elements_per_weight * weight))
        
        # Calculate the end index for this split
        end_index = start_index + num_elements
        
        # Split the series for this part and append to the list
        split_lists.append(series[start_index:end_index].tolist())
        
        # Update the start index for the next split
        start_index = end_index
        
    return split_lists


def run_func_with_nextflow(my_func, my_iterable, log_file_path, partition = 'day', clear_work_dir = True, return_output = True):
    """
    Executes a given Python function on an iterable of objects using Nextflow, optionally returning the results.

    This function serializes the provided Python function and the iterable objects, then uses Nextflow to distribute 
    and execute the function on each object in a parallel or distributed environment. The Nextflow and Python helper 
    scripts are specified within the function. The execution log can be directed to a specified log file.

    Parameters:
    - my_func (callable): The Python function to execute on each item of the iterable. This function should be serializable 
      with pickle.
    - my_iterable (iterable): An iterable of objects that the `my_func` will be applied to. Each object in the iterable 
      should be serializable with pickle.
    - return_output (bool, optional): If True, the function returns a list of results from applying `my_func` to the 
      iterable objects. If False, no results are returned. Defaults to True.
    - log_file_path (str, optional): The file path for the Nextflow log file. If not provided, logs are redirected to 
      '/dev/null', effectively discarding them.

    Returns:
    - list: A list of results from the function execution if `return_output` is True; otherwise, None.

    Raises:
    - subprocess.CalledProcessError: If the Nextflow command execution fails.
    - Exception: If any other unexpected error occurs during the function's execution.
    
    Note:
    - This function assumes access to the necessary Nextflow scripts and configurations defined within 'ghoshtools'.
    - It's important that `my_func` and the objects in `my_iterable` are correctly serializable with pickle, as they 
      are passed to Nextflow through serialized files.
    - The function uses global settings from `GT_GLOBALS` for the scratch directory and Conda environment YAML path.
    """
    
    if clear_work_dir:
        safe_clear_work_dir()
        

    partition_weighting = {
        'bigmem': 3,
        'ycga_bigmem': 1,
        'scavenge' : 2,
        # Add new partitions here as needed with their weightings
    }

    if isinstance(partition, list):
        weighted_iterable = []
        chunked_iterable = split_series_by_weight(pd.Series(my_iterable), partition = partition, weighting_dict = partition_weighting)
        pprint(chunked_iterable)
            
        with ProcessPoolExecutor() as executor:
            futures = []
            for part, chunk_iter in zip(partition, chunked_iterable):
                future = executor.submit(run_func_with_nextflow, my_func, chunk_iter, log_file_path, partition=part, clear_work_dir=False, return_output=return_output)
                futures.append(future)
            
            # If you need to process results
            results = [future.result() for future in futures]
        return results


    if log_file_path is None:
        logger.warn("No log file path provided. Redirecting to /dev/null")
        log_file_path = '/dev/null'
    else:
        log_file_path = append_partition_to_log_filename(log_file_path, partition)
        utils.run_shell_command(f"> {log_file_path}")
        logger.info("Log file set to %s", log_file_path)
    
    
    with tempfile.NamedTemporaryFile(mode="wb", delete=False, dir=GT_GLOBALS.SCRATCH_DIR, suffix=".py") as func_file_path:
        # pickle.dump(my_func, func_file_path)
        # func_file_path.flush()
        # TODO: Assumes that all methods in this file have different names
        python_cmd = f"python /home/rg972/project/GhoshTools/ghoshtools/extract_method.py --input {my_func.__code__.co_filename} --func_name {my_func.__name__} --output {func_file_path.name}"
        utils.run_shell_command(python_cmd)
        
        with tempfile.TemporaryDirectory(dir=GT_GLOBALS.SCRATCH_DIR) as temp_iterable_dir_path:
            with ProcessPoolExecutor() as executor:
                executor.map(partial(pickle_dump_iterable, temp_iterable_dir_path = temp_iterable_dir_path), my_iterable)
                logger.info("%d elements from iterable were pickled", len(my_iterable))
                
                with resources.path('ghoshtools', 'work') as work_dir_path:
                    work_dir_path = os.path.join(GT_GLOBALS.SCRATCH_DIR, '../work')
                    # TODO: If multiple calls to this work directory are made at the same time, the files will be overwritten.
                    
                    with resources.path('ghoshtools.resources', 'run_python_function.nf') as nextflow_script_file_path:
                        with resources.path('ghoshtools.resources', 'nextflow_helper_script.py') as python_script_file_path:                              
                            nextflow_cmd = f"nextflow -log {log_file_path} run {nextflow_script_file_path} --return_output {return_output} --python_path {python_script_file_path} --file_path {func_file_path.name} --dir_path {temp_iterable_dir_path} -w {work_dir_path} -profile {partition}"
                            print(nextflow_cmd)
                            os.system(f"echo {nextflow_cmd} > /home/rg972/project/nextflow_command.txt")
                            result = subprocess.run(nextflow_cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                            
                            if return_output:
                                results = [load_pickle_obj_from_file(result_file_path) for result_file_path in convert_nf_output_to_list_of_pickle_files(result.stdout)]
                                logger.info("Nextflow run complete. %d results generated. Log file available at %s", len(results), log_file_path)
                                return results
                            
    logger.info("Nextflow run complete. No output returned. Log file available at %s", log_file_path)
    return

def binary_search_optimal_batch_size(min_batch_size, max_batch_size, tolerance, max_iterations, measure_performance):
    """
    Finds the optimal batch size using a binary search algorithm.
    
    Args:
    - min_batch_size (int): The minimum batch size to start the search.
    - max_batch_size (int): The maximum batch size to start the search.
    - tolerance (float): The tolerance level for improvement in execution time to decide on convergence.
    - max_iterations (int): Maximum number of iterations to prevent infinite loops.
    - measure_performance (function): A function that takes a batch size as input and returns the execution time.
    
    Returns:
    - int: The optimal batch size.
    """
    
    optimal_batch_size = min_batch_size
    ic(optimal_batch_size)
    for _ in range(max_iterations):
        current_batch_size = (min_batch_size + max_batch_size) // 2
        ic(current_batch_size)
        execution_time_current = measure_performance(current_batch_size)
        ic(execution_time_current)
        
        # Check the performance of a slightly smaller batch size
        execution_time_smaller = measure_performance(current_batch_size - 1)
        ic(execution_time_smaller)
        
        # If the smaller batch size is better within the tolerance, search in the lower half
        if execution_time_smaller < execution_time_current * (1 - tolerance):
            max_batch_size = current_batch_size - 1
        else:
            # Otherwise, the current or a larger batch size might be better, so search in the upper half
            optimal_batch_size = current_batch_size
            min_batch_size = current_batch_size + 1
        
        # If the difference between min and max batch size is within the tolerance, we've found our optimal size
        if max_batch_size - min_batch_size <= 1:
            break
    
    return optimal_batch_size

