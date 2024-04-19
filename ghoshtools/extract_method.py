import argparse
import os
import re

#TODO: Can't handle local imports

def extract_and_rewrite(input_path, function_name, output_path):
    with open(input_path, 'r') as file:
        lines = file.readlines()
    
    # Compile a regular expression to find the function definition
    function_def_regex = re.compile(rf'def\s+{function_name}\s*\((.*?)\)\s*:')
    main_block_regex = re.compile(r'if\s+__name__\s*==\s*\'__main__\':')
    inside_function = False
    function_code = []
    other_code = []
    skip_main_block = False
    
    for line in lines:
        if main_block_regex.match(line):
            skip_main_block = True  # Start skipping lines
            continue  # Skip the current line
        elif skip_main_block:
            if line.strip() == '':
                continue  # Skip empty lines within the main block
            elif line.startswith('    '):  # Assuming indentation with four spaces
                continue  # Skip indented lines within the main block
            else:
                skip_main_block = False  # Non-indented line means end of main block
        
        if function_def_regex.match(line):
            inside_function = True
            function_code.append(line)
        elif inside_function:
            if line.startswith('def '):
                inside_function = False
                other_code.append(line)
            else:
                function_code.append(line if line.strip() != '' else '\n')
        elif not skip_main_block:
            other_code.append(line)
    
    if not function_code:
        raise ValueError(f"Function {function_name} not found in {input_path}")
    
    if_main_block = "if __name__ == \"__main__\":\n    pass\n"
    
    with open(output_path, 'w') as output_file:
        output_file.writelines([function_code[0]] + [f'    {line}' for line in other_code] + function_code[1:] + ['\n'] + [if_main_block])

def main():
    parser = argparse.ArgumentParser(description="Extract a function from a Python file.")
    parser.add_argument('--input', help='Path to the .py file')
    parser.add_argument("--func_name", help="Name of the function to extract.")
    parser.add_argument("--output", help="Path to the output file.")
    
    args = parser.parse_args()
    
    if not args.input.endswith('.py'):
        parser.error("Input file must be a .py file")
    
    if not os.path.exists(args.input):
        parser.error("Input file does not exist")
    
    try:
        extract_and_rewrite(args.input, args.func_name, args.output)
        print(f"Function {args.func_name} has been extracted and rewritten to {args.output}")
    except ValueError as e:
        print(e)

if __name__ == "__main__":
    main()
