import asyncio

from src.engineering_1_processing import (
    temperature_stats,
    hottest_coldest,
    anomaly_detection,
    rainfall_humidity_stats,
)


async def async_temperature(df):
    await asyncio.sleep(0)
    return {"temperature_stats": temperature_stats(df)}


async def async_extremes(df):
    await asyncio.sleep(0)
    return {"extremes": hottest_coldest(df)}


async def async_anomalies(df):
    await asyncio.sleep(0)
    return {"anomalies": anomaly_detection(df)}


async def async_rainfall(df):
    await asyncio.sleep(0)
    return {"rainfall_stats": rainfall_humidity_stats(df)}


async def run_concurrent_tasks(df):

    results = await asyncio.gather(
        async_temperature(df),
        async_extremes(df),
        async_anomalies(df),
        async_rainfall(df)
    )


    final_result = {}
    for res in results:
        final_result.update(res)

    return final_result













































































































































































