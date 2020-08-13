from multiprocessing import Pool
import os, time, random, logging


def long_time_task(name, x):
    print('Run task %s (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(random.random() * 3)
    end = time.time()
    print('Task %s runs %0.2f seconds.' % (name, (end - start)))

def main():
    print('Parent process %s.' % os.getpid())
    p = Pool()
    for i in range(17):
        p.apply_async(long_time_task, args=(i, 1))
    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')

main()