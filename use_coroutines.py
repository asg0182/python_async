import asyncio
import aiohttp
import time
import yaml


BASE_URL = "http://127.0.0.1:5000"


def tasks_init(config="tasks.yml"):
    with open(config) as f:
        data = yaml.safe_load(f)
    return data


async def start_job(session, url):
    try:
        async with session.post(url, timeout=20) as resp:
            if resp.ok:
                return await resp.json()
    except Exception as e:
        print(e)


async def start_jobs(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [start_job(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    

async def get_status(session, url):
    try:
        async with session.get(url, timeout=20) as resp:
            if resp.ok:
                return await resp.json()
    except Exception as e:
        print(e)
    

async def wait_for_status(session, url, status="success"):
    while True:
        data = await get_status(session, url)
        if data.get("status") == status:
            return data
        print(f"GET - {url} - {str(data)}")
        await asyncio.sleep(1)


async def wait_for_jobs(urls):
    async with aiohttp.ClientSession() as session:
        tasks = [wait_for_status(session, url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results


async def main():
    data = tasks_init()
    start_time = time.time()
    urls = [f"{BASE_URL}/jobs/{job['job_id']}" for job in data]
    results = await start_jobs(urls)

    urls = [f"{BASE_URL}/requests/{r['id']}" for r in results]
    print("\n".join(urls))
    results = await wait_for_jobs(urls)

    end_time = time.time()

    print(results)

    print(f"Total time: {end_time - start_time:.2f} seconds")


asyncio.run(main())
