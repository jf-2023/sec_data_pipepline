import os
import json
import time
import asyncio
import aiofiles
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def fetch_all_files(directory, request_limit):
    """Opens all files in a selected directory synchronously"""
    files = os.listdir(directory)
    for file in tqdm(files[:request_limit], desc='Synchronous Progress'):
        full_path = os.path.join(directory, file)
        try:
            with open(full_path, "r") as f:
                json.load(f)
        except Exception as e:
            print(f"Error opening file {full_path}: {e}")


async def fetch_file_async(file_path):
    """Helper function to fetch a single file asynchronously"""
    try:
        async with aiofiles.open(file_path, "r") as f:
            data = await f.read()
            json.loads(data)
    except Exception as e:
        print(f"Error opening file {file_path}: {e}")


async def fetch_all_files_async(directory, request_limit):
    """Opens all files in a selected directory asynchronously"""
    files = os.listdir(directory)
    tasks = []
    for file in tqdm(files[:request_limit], desc='Asynchronous Progress'):
        full_path = os.path.join(directory, file)
        tasks.append(fetch_file_async(full_path))
    await asyncio.gather(*tasks)


def fetch_file(file_path):
    """Helper function to fetch a single file for threading and multiprocessing"""
    try:
        with open(file_path, "r") as f:
            json.load(f)
    except Exception as e:
        print(f"Error opening file {file_path}: {e}")


def fetch_all_files_threading(directory, request_limit):
    """Opens all files in a selected directory using threading"""
    files = os.listdir(directory)
    with ThreadPoolExecutor() as executor:
        list(tqdm(executor.map(fetch_file,
                               [os.path.join(directory, file) for file in files[:request_limit]]),
                  total=request_limit, desc='Threading Progress'))


def fetch_all_files_multiprocessing(directory, request_limit):
    """Opens all files in a selected directory using multiprocessing"""
    files = os.listdir(directory)
    with ProcessPoolExecutor() as executor:
        list(tqdm(executor.map(fetch_file,
                               [os.path.join(directory, file) for file in files[:request_limit]]),
                  total=request_limit, desc='Multiprocessing Progress'))


def main():
    company_data_dir = "C:\\Users\\cornf\\Documents\\companyFacts"
    num_requests = 1700

    print("Starting synchronous requests...")
    start_time = time.perf_counter()
    fetch_all_files(company_data_dir, num_requests)
    duration_sync = time.perf_counter() - start_time
    print(f"Synchronous: {duration_sync:.2f} seconds")

    print("Starting asynchronous requests...")
    start_time = time.perf_counter()
    asyncio.run(fetch_all_files_async(company_data_dir, num_requests))
    duration_async = time.perf_counter() - start_time
    print(f"Asynchronous: {duration_async:.2f} seconds")

    print("Starting threading requests...")
    start_time = time.perf_counter()
    fetch_all_files_threading(company_data_dir, num_requests)
    duration_threading = time.perf_counter() - start_time
    print(f"Threading: {duration_threading:.2f} seconds")

    print("Starting multiprocessing requests...")
    start_time = time.perf_counter()
    fetch_all_files_multiprocessing(company_data_dir, num_requests)
    duration_multiprocessing = time.perf_counter() - start_time
    print(f"Multiprocessing: {duration_multiprocessing:.2f} seconds")


if __name__ == "__main__":
    main()
