#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.file_path = '' // Default empty, expecting user to provide
params.dir_path = '' // Default empty, expecting user to provide

// Validate parameters
if (params.file_path.trim() == '') {
    error "No file_path provided! Use --file_path to specify."
}

if (params.dir_path.trim() == '') {
    error "No dir_path provided! Use --dir_path to specify."
}

process RunPythonFunc {
    input:
    path(dir_file)

    output:
    path "*.pkl"

    beforeScript "env -i bash -c 'source /vast/palmer/home.mccleary/rg972/.bash_profile'"
    
    script:
    """
    ml miniconda
    conda activate poop

    # Execute the constructed commands in parallel
    python ${params.python_path} --pickled_func_file_path ${params.file_path} --pickled_obj_file_path ${dir_file} --return_output ${params.return_output}
    """
}


workflow {
    dir_files_ch = Channel.fromPath("${params.dir_path}/*")
    RunPythonFunc(dir_files_ch)
}
