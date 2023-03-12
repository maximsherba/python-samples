"""
    Asyncronously execute in a DB a list of queries from file 
    using a pool of connections and synchronous db driver.
    Example is based on Clickhouse DB.
"""
import asyncio
from functools import wraps, partial
import time
from clickhouse_driver import Client
import configparser
import logging


# Enable INFO logging level
logging.getLogger().setLevel(logging.INFO)

# Read DB connection parameters
config = configparser.ConfigParser()
config.read('DB_config.ini')
config.sections()

DB_URL = config['CLICKHOUSE']['DB_URL']
DB_NAME = config['CLICKHOUSE']['DB_NAME']
USERNAME = config['CLICKHOUSE']['USERNAME']
PASSWORD = config['CLICKHOUSE']['PASSWORD']

DB_DICT = {
    'DB_CLICK': {
        'host': DB_URL,
        'user': USERNAME,
        'password': PASSWORD,
        'database': DB_NAME
    }
}

CONNECTION_POOL_SIZE = 10
FILE_READ = 'sql_queries.sql'
SLEEP_TIMER = 3


def async_wrap(func):
    # Decorator which transforms syncronous function in asyncronous
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)
    return run 


async def async_job(func, args, kwargs):
    # Execution of sincronous function with parameters in async mode
    async_func = async_wrap(func)
    await async_func(args, kwargs)
    return func
    

async def worker(task_queue, connection_queue, parameters):
    # Individual worker task - sequentially process tasks as they come into the queue
    while True:
        # Taking query from the task queue
        query = await task_queue.get()
        if query is None:
            break
        # Taking connection from the connection queue
        conn = await connection_queue.get()
        # Connection as a function, query as a parameter, parameters should be passed
        try:
            result = await async_job(conn, query, parameters)
        except:
            await task_queue.put(query)
            time.sleep(SLEEP_TIMER)
        connection_queue.task_done()
        task_queue.task_done()
        # Put back released connection to the connection queue
        await connection_queue.put(conn)
        # In order to be finished this function should return smth
        return None 


async def main():
    parameters = {'with_column_types': True, 'external_tables': None}
    task_queue = asyncio.Queue()
    connection_queue = asyncio.Queue(CONNECTION_POOL_SIZE)
    
    # Initial filling of connection queue
    clients = [None]*CONNECTION_POOL_SIZE
    for i in range(CONNECTION_POOL_SIZE):
        clients[i] = Client(**DB_DICT['DB_CLICK'])
        await connection_queue.put(clients[i].execute)           

    # Wait a bit to make sure connections are created
    time.sleep(1)
    
    # Initial filling of the task queue from file   
    cnt_tasks = 0           
    with open(FILE_READ, 'r') as file:
        for line in file:
            line = line.rstrip('\n')
            cnt_tasks += 1
            await task_queue.put(line)         
    await task_queue.put(None) 
    
    workers = [asyncio.create_task(worker(task_queue, connection_queue, parameters)) 
               for _ in range(cnt_tasks)]

    await asyncio.gather(*workers)

    for i in range(CONNECTION_POOL_SIZE):
        clients[i].disconnect()


if __name__ == "__main__":    
    start = time.time()
    logging.info(f"Start time: {start}")    
    asyncio.run(main())
    end = time.time()
    logging.info(f"Time elapse: {end-start}")

