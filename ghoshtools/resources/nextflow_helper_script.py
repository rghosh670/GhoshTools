import os
import dill as pickle
import argparse
import tempfile
import importlib.util
import sys
import re

def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def load_module_from_path(file_path):
    module_name = os.path.basename(file_path).rstrip('.py')
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def load_pickle_files_and_write_nextflow_script(pickled_func_file_path, pickled_obj_file_path, return_output):
    module = load_module_from_path(pickled_func_file_path)
    my_func_name = dir(module)[-1]
    my_func = getattr(module, my_func_name, None)
        
    with open(pickled_obj_file_path, 'rb') as f:
        my_obj = pickle.load(f)
    
    current_dir = os.getcwd()
    with tempfile.NamedTemporaryFile(delete=False, dir=current_dir, suffix='.pkl') as result_file_path:
        result = my_func(my_obj)
        
        if return_output:
            pickle.dump(result, result_file_path)
            result_file_path.flush()
    
    return result_file_path.name
    
def main():
    parser = argparse.ArgumentParser(description='Load pickle files and write nextflow script')
    parser.add_argument('--pickled_func_file_path', type=str, help='Path to the pickled function file')
    parser.add_argument('--pickled_obj_file_path', type=str, help='Path to the pickled object file')
    parser.add_argument('--return_output', type=str2bool, help='True if user wants output, false if not.')
    args = parser.parse_args()
    
    result_file_path = load_pickle_files_and_write_nextflow_script(args.pickled_func_file_path, args.pickled_obj_file_path, args.return_output)
    print(result_file_path)
    

if __name__ == '__main__':
    main()