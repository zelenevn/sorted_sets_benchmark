from functools import lru_cache
from redis.asyncio import Redis
import asyncio
import random
from time import perf_counter


read_counter = []
update_counter = []


@lru_cache
async def get_connection():
    return Redis(host="localhost", port=6379, decode_responses=True)
    
    
async def create_players():
    r = await get_connection()
    pipe = r.pipeline()
    for id in range(20000000):
        pipe.zadd("raiting", {id: random.randint(0, 1000000)})
        if id % 10000 == 0:
                await pipe.execute()
                
async def read_raitings(r):
    try:
        while True:
            start = perf_counter()
            top_1000_players = await r.zrevrange('leaderboard', 0, 999, withscores=True)
            end = perf_counter()
            read_counter.append(end - start)
    except asyncio.CancelledError:
        return
        
async def update_random_raiting(r):
    try:
        while True:
            id = random.randint(0, 1000000)
            start = perf_counter()
            await r.zincrby('leaderboard', random.randint(0, 10000), id)
            end = perf_counter()
            update_counter.append(end - start)
    except asyncio.CancelledError:
        return
    
    
async def main():
    tasks = []
    r = await get_connection()
    for i in range(1000):
        task = asyncio.create_task(read_raitings(r))
        tasks.append(task)
        
        task = asyncio.create_task(update_random_raiting(r))
        tasks.append(task)
        
    try:
        await asyncio.sleep(10)
    finally:
        for task in tasks:
            task.cancel()
        
    await asyncio.gather(*tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
    print(f"Read avg: {sum(read_counter) / len(read_counter)}")
    print(f"Read max: {max(read_counter)}")
    print(f"Read min: {min(read_counter)}")
    
    print(f"Update avg: {sum(update_counter) / len(update_counter)}")
    print(f"Update max: {max(update_counter)}")
    print(f"Update min: {min(update_counter)}")
    