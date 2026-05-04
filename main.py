import time
import json
import os
import asyncio

from utils import *
from src.engineering_1_processing import *

from src.engineering_2_asyncio import run_concurrent_tasks

def main():
    df = load_data()
    df = rolling_avg(df, window=2)

    temp_stats = temperature_stats(df)
    extremes = hottest_coldest(df)
    anomalies = anomaly_detection(df)
    rainfall_stats = rainfall_humidity_stats(df)

    print("\nENGINEERING 1 OUTPUT")
    print("Temperature Stats:", temp_stats)
    print("Extremes:", extremes)
    print("Anomalies:", anomalies)
    print("Rainfall & Humidity:", rainfall_stats)

    return df

async def run_engineering_2(df):
    global_results = await run_concurrent_tasks(df)
    output_path_2 = os.path.join(BASE_DIR, "output_engineering_2.json")

    with open(output_path_2, "w") as f:
        json.dump(global_results, f, indent=4, default=str)

    print("\nEngineering 2 Output Saved:", output_path_2)


if __name__ == "__main__":
    start = time.time()

    df = main()
    asyncio.run(run_engineering_2(df))

    end = time.time()
    print("\nExecution Time:", end - start)