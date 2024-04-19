import ghoshtools as gt
from pprint import pprint

def my_func(x):
    import time
    x = x**2
    time.sleep(10)
    return x
    
def test_one():    
    my_iterable = list(range(5))
    results = gt.run_func_with_nextflow(my_func, my_iterable, log_file_path = '/home/rg972/project/Poop/nextflow_poop.log')
    pprint(results)
    return

def main():
    test_one()

if __name__ == '__main__':
    main()