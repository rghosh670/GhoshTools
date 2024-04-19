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
    val(tuples)

    output:
    path "*.pkl"

    script:
    // Ensure tuples is treated as a list of tuples; encapsulate single tuple in a list if necessary
    def cmdList = (tuples instanceof List && tuples.every{ it instanceof List }) ? tuples : [tuples]
    def commands = cmdList.collect { tuple ->
        // Construct command for each tuple, ensuring tuple is a list with expected elements
        if(tuple.size() >= 4) {
            return "python ${tuple[1]} --pickled_func_file_path ${tuple[0]} --pickled_obj_file_path ${tuple[3]} &&"
        } else {
            println("Warning: Tuple does not have the expected number of elements: ${tuple}")
            return ""
        }
    }.join(' ').trim()
    
    // Using triple double-quotes for multi-line strings in Groovy interpolation
    """
    # Prepare the environment
    env -i bash -c 'source /vast/palmer/home.mccleary/rg972/.bash_profile'
    ml miniconda
    conda activate poop

    # Execute the constructed commands in parallel
    ${commands}

    # Wait for all background jobs to complete
    wait
    """
}


workflow {
    // Create a channel for all files in the directory path and group them into chunks of 3
    dir_files_ch = Channel
                    .fromPath("${params.dir_path}/*")
                    .map { obj_file_path ->
                        [params.file_path, params.python_path, params.return_output, obj_file_path.toAbsolutePath().toString()]
                    }
                    .buffer(size: 3, remainder: true) // Group files into chunks of 3

    RunPythonFunc(dir_files_ch)
}
