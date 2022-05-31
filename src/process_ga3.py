from lib.ua import UniversalAnalytics, AnalyticsQuery, AnalyticsReport
from google.cloud import bigquery
import datetime
import pandas as pd
import json
import os


def get_ga3(to_table_id, client, ga3_view_id, pull_start_date, website_url):


    def get_end_data():
        sql = """SELECT MIN(date) FROM `{to_table_id}`""".format(to_table_id=to_table_id)

        result = client.query(sql)
        df2 = result.to_dataframe()
        first_date = df2.iloc[0]['f0_']
        pull_end_date = (first_date - datetime.timedelta(days=1))

        return pull_end_date

    def get_report(ua, ga3_view_id, pull_start_date, pull_end_date, pageToken):
        dimensions = [
            'ga:date',
            'ga:landingPagePath',
            'ga:country',
            'ga:region',
            'ga:city',
            'ga:source',
            'ga:medium',
            'ga:campaign',
        ]

        metrics = [
            'ga:users',
            'ga:newUsers',
            'ga:entrances',
            'ga:sessions',
            'ga:pageviews',
            'ga:uniquePageviews',
            'ga:timeOnPage',
            #{ga:conversion_event},
            'ga:transactionRevenue',
            'ga:transactions',
        ]

        query = (
            AnalyticsQuery(ua, ga3_view_id)
            .date_range([(pull_start_date, pull_end_date)])
            .dimensions(dimensions)
            .metrics(metrics)
            .page_size(10000)
            .page_token(pageToken)
        )
        response = query.get().raw
        return response


    def get_token(response):
        for report in response.get('reports', []):
            pageToken = report.get('nextPageToken', None)
        return pageToken


    def dict_transfer(response, mylist):
        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])
            for row in rows:
                dict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                for header, dimension in zip(dimensionHeaders, dimensions):
                    dict[header] = dimension

                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metricHeaders, values.get('values')):
                        if ',' in value or '.' in value:
                            dict[metric.get('name')] = float(value)
                        elif value == '0.0':
                            dict[metric.get('name')] = int(float(value))
                        else:
                            dict[metric.get('name')] = int(value)
                mylist.append(dict)


    ua = UniversalAnalytics('/content/service.json')
    pull_end_date = get_end_data()

    mylist = []
    pageToken = "0"

    print("Pulling GA3 data...")

    while pageToken != None:
        response = get_report(ua, ga3_view_id, pull_start_date, pull_end_date, pageToken)
        pageToken = get_token(response)
        dict_transfer(response, mylist)
    else:
        print("GA3 data download complete")

    print("Cleaning up data...")
    df = pd.DataFrame(mylist)
    print(df.head())
    df['ga:landingPagePath'].loc[df['ga:landingPagePath'] != "(not set)"] = website_url + df['ga:landingPagePath'].loc[df['ga:landingPagePath'] != "(not set)"].astype(str)
    df['ga:date'] = pd.to_datetime(df['ga:date']).dt.date


    order = {
    'ga:date': 'date',
    'ga:landingPagePath': 'landing_page',
    'ga:country': 'country',
    'ga:region': 'region',
    'ga:city': 'city',
    'ga:source': 'utm_source',
    'ga:medium': 'utm_medium',
    'ga:campaign': 'utm_campaign',
    'ga:users': 'users',
    'ga:newUsers': 'new_users',
    'ga:entrances': 'entrances',
    'ga:sessions': 'sessions',
    'ga:pageviews': 'page_views',
    'ga:uniquePageviews': 'unique_page_views',
    'ga:timeOnPage': 'engagment_time_sec_per_session',
    #'{ga:conversion_event}: 'conversions',
    'ga:transactionRevenue': 'ecommerce_revenue',
    'ga:transactions': 'ecommerce_transactions',
    }

    df = df[order.keys()].rename(columns=order)

    print("Uploading to BigQuery")


    job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    schema=[
                bigquery.SchemaField("date", "DATE"),
        ],
      )

    job = client.load_table_from_dataframe(
        df,
        destination=to_table_id,
        job_config=job_config
    )

    job.result()

    print("Query results loaded to the table {}".format(to_table_id))
