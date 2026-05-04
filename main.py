import asyncio
import time
import json

from src.utils import load_data
from src.engineering_1_processing import rolling_avg
from src.engineering_2_asyncio import run_concurrent_tasks
from src.engineering_3_parallel import parallel_processing, generate_output


async def main():
    df = load_data("weather_data.csv")
    df = rolling_avg(df, window=2)
    global_results = await run_concurrent_tasks(df)

    # Parallel Part - Done by Vaibhav
    city_results = await parallel_processing(df)
    final_output = generate_output(global_results, city_results)

    print("\n===== FINAL OUTPUT =====")
    print(json.dumps(final_output, indent=4))

    # Save to output file
    with open('output/final_output.json', 'w') as f:
        json.dump(final_output, f, indent=4)
    print("\nOutput saved to output/final_output.json")


if __name__ == "__main__":
    start = time.time()
    asyncio.run(main())
    end = time.time()
    print("\nExecution Time:", end - start)
