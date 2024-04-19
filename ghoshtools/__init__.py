"""Top-level package for GhoshTools."""

__author__ = """Rohit Ghosh"""
__email__ = 'rohit.ghosh@yale.edu'
__version__ = '0.1.0'

import logging
import os
import sys
from importlib import resources
from ghoshtools import utils
from pathlib import Path
from ghoshtools.globals import Globals

GT_GLOBALS = Globals()

from ghoshtools.ghoshtools import (
    run_func_with_nextflow,
    binary_search_optimal_batch_size,
)

# Configure package-level logger.
_logger = logging.getLogger('ghoshtools')
_logger.setLevel(logging.DEBUG)  # Set default logging level

# Create console handler and set level to debug
_ch = logging.StreamHandler()
_ch.setLevel(logging.DEBUG)

# Create formatter
_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# Add formatter to ch
_ch.setFormatter(_formatter)

# Add ch to logger
_logger.addHandler(_ch)


def set_conda_environment(conda_yml_file_path = None):
    # if 'CONDA_DEFAULT_ENV' not in os.environ and conda_yml_file_path is None:
    #     _logger.info("No Conda environment is activated. Using default Conda environment YML file.")
    #     conda_yml_file_path = '/home/rg972/project/GhoshTools/ghoshtools/resources/poop.yml'
    # elif conda_yml_file_path is None:
    #     current_conda_env = os.path.dirname(os.path.dirname(sys.executable))
    #     conda_env_name = os.path.basename(current_conda_env)
    #     conda_yml_file_name = f'{conda_env_name}.yml'
        
    #     _logger.info(f"No yml file passed in. Exported current conda environment ({conda_yml_file_name})")

    #     with resources.path("ghoshtools", "resources") as resources_dir_path:
    #         conda_yml_file_path = os.path.join(resources_dir_path, conda_yml_file_name)
    #         export_cmd = f"conda env export > {conda_yml_file_path}"
    #         utils.run_shell_command(export_cmd)
            
    GT_GLOBALS.CONDA_YML = conda_yml_file_path
    return
        
set_conda_environment()

__all__ = [
    "CONDA_YML",
    "run_func_with_nextflow",
    "binary_search_optimal_batch_size",
]