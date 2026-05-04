import asyncio

from src.engineering_1_processing import (
    temperature_stats,
    hottest_coldest,
    anomaly_detection,
    rainfall_humidity_stats
)


async def process_city(city, df):

    city_df = df[df['city'] == city]

    temp_stats = temperature_stats(city_df)
    extremes = hottest_coldest(city_df)
    anomalies = anomaly_detection(city_df)
    rainfall = rainfall_humidity_stats(city_df)
    avg_temp = temp_stats['avg_temp'][city]

    summary = {
        "average_temperature": round(avg_temp, 2),
        "total_rainfall": rainfall['rainfall_mm_sum'][city],
        "anomaly_count": len(anomalies[city]),
        "climate_type": "Hot" if avg_temp > 30 else "Moderate"
    }

    return {
        "city": city,
        "summary": summary,
        "detailed": {
            "temperature": temp_stats,
            "extremes": extremes,
            "anomalies": anomalies,
            "rainfall": rainfall
        }
    }

async def parallel_processing(df):

    tasks = []

    for city in df['city'].unique():
        tasks.append(process_city(city, df))

    results = await asyncio.gather(*tasks)

    return results

def generate_output(global_results, city_results):

    final_output = {
        "status": "success",
        "timestamp": str(asyncio.get_event_loop().time()),
        "global_analysis": global_results,
        "cities": city_results,
        "insights": generate_insights(city_results)
    }

    return final_output


def generate_insights(city_results):

    insights = []

    for city_data in city_results:

        city = city_data['city']
        summary = city_data['summary']

        insight = f"{city} has avg temp {summary['average_temperature']}°C " \
                  f"with {summary['anomaly_count']} anomalies and " \
                  f"{summary['climate_type']} climate."

        insights.append(insight)

    return insights