from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account
import pandas as pd
import json

# Initialize the client
KEY_FILE_LOCATION = 'key.json'

credentials = service_account.Credentials.from_service_account_file(KEY_FILE_LOCATION)
client = BetaAnalyticsDataClient(credentials=credentials)
def fetch_ga4_data_batch(property_id, metrics, dimensions):

    request = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date="2024-01-01", end_date="2024-01-30")],
        metrics=metrics,
        dimensions=dimensions
    )
    return client.run_report(request)
def process_data_to_excel(responses, filename):
    # Convert each response to a DataFrame and merge them
    dfs = []
    for response in responses:
        data = [
            {
                'date': row.dimension_values[0].value,
                **{metric.name: row.metric_values[i].value for i, metric in enumerate(response.metric_headers)}
            }
            for row in response.rows
        ]
        dfs.append(pd.DataFrame(data))

    # Merge DataFrames on 'date' column
    final_df = dfs[0]
    for df in dfs[1:]:
        final_df = pd.merge(final_df, df, on='date', how='outer')

    # Export to Excel and CSV with specified filename
    final_df.to_excel(f"{filename}.xlsx", index=False)
    final_df.to_csv(f"{filename}.csv", index=False)

# Define metrics in two batches
metrics_batch_1 = [Metric(name="engagementRate"), Metric(name="dauPerMau"),Metric(name="activeUsers"),Metric(name="totalRevenue"),
                   Metric(name="eventCount"),Metric(name="bounceRate"),Metric(name="newUsers"),Metric(name="sessionsPerUser"),Metric(name="totalAdRevenue"),
                   Metric(name="crashFreeUsersRate")]  # First 10 metrics
metrics_batch_2 = [Metric(name="averageRevenuePerUser"),
                 Metric(name="crashAffectedUsers"),
                 Metric(name="dauPerWau"),
                 Metric(name="grossPurchaseRevenue"),
                 Metric(name="screenPageViewsPerUser"),
                 Metric(name="purchaseRevenue"),
                 Metric(name="userEngagementDuration"),
                 Metric(name="screenPageViews"),

                   ]  # Next 10 metrics
# Define dimensions
dimensions = [
    Dimension(name="date"),
    #Dimension(name="country"),
    #Dimension(name="platform"),
    Dimension(name="adUnitName"),
    #Dimension(name="adSourceName"),
    #Dimension(name="appVersion"),
    #Dimension(name="operatingSystem")
]
property_ids = {
    '#ad your property id#': 'Smarty-Jacket',
    '#ad your property id#': 'Screen-Cast'
}


for property_id, filename in property_ids.items():
    # Fetch data in batches
    response_batch_1 = fetch_ga4_data_batch(property_id, metrics_batch_1, dimensions)
    response_batch_2 = fetch_ga4_data_batch(property_id, metrics_batch_2, dimensions)
    # Process and export to Excel
    process_data_to_excel([response_batch_1, response_batch_2], filename)
