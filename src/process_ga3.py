from lib.ua import UniversalAnalytics, AnalyticsQuery, AnalyticsReport
from google.cloud import bigquery
from typing import Union, List, Dict
from tqdm import tqdm
import datetime
import pandas as pd
import json
import os


def get_ga3(
    to_table_id: str,
    client: bigquery.client.Client,
    ga3_view_id: str,
    pull_start_date: str,
    goal_metric: str = "ga:goalCompletionsAll",
) -> None:

    def get_report(
        ua: UniversalAnalytics,
        ga3_view_id: str,
        pull_start_date: str,
        pull_end_date: datetime.date,
        pageToken: str,
    ) -> Dict[str, Union[str, float, int]]:

        dimensions = [
            'ga:date',
            'ga:hostname',
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
            goal_metric,
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
            .sampling_level("LARGE")
        )
        response = query.get().raw
        return response


    def get_token(
        response: Dict[str, Union[str, float, int]],
    ) -> str:

        for report in response.get('reports', []):
            pageToken = report.get('nextPageToken', None)
        return pageToken


    def dict_transfer(
        response: Dict[str, Union[str, float, int]],
        mylist: Union[List, List[Dict[str, Union[str, float, int]]]]
    ) -> None:

        for report in response.get('reports', []):
            columnHeader = report.get('columnHeader', {})
            dimensionHeaders = columnHeader.get('dimensions', [])
            metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
            rows = report.get('data', {}).get('rows', [])
            for row in rows:
                row_dict = {}
                dimensions = row.get('dimensions', [])
                dateRangeValues = row.get('metrics', [])

                for header, dimension in zip(dimensionHeaders, dimensions):
                    row_dict[header] = dimension

                for i, values in enumerate(dateRangeValues):
                    for metric, value in zip(metricHeaders, values.get('values')):
                        if ',' in value or '.' in value:
                            row_dict[metric.get('name')] = float(value)
                        elif value == '0.0':
                            row_dict[metric.get('name')] = int(float(value))
                        else:
                            row_dict[metric.get('name')] = int(value)
                mylist.append(row_dict)


    ua = UniversalAnalytics('/content/service.json')

    sql = """SELECT MIN(date) FROM `{to_table_id}`""".format(to_table_id=to_table_id)

    result = client.query(sql)
    df2 = result.to_dataframe()
    first_date = df2.iloc[0]['f0_']
    pull_end_date = (first_date - datetime.timedelta(days=1))

    def last_day_of_month(any_day):
        next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
        return next_month - datetime.timedelta(days=next_month.day)

    def monthlist(begin,end):
        begin = datetime.datetime.strptime(begin, "%Y-%m-%d")
        end = datetime.datetime.combine(end, datetime.time.min)

        result = []
        while True:
            if begin.month == 12:
                next_month = begin.replace(year=begin.year+1,month=1, day=1)
            else:
                next_month = begin.replace(month=begin.month+1, day=1)
            if next_month > end:
                break
            result.append ((begin.strftime("%Y-%m-%d"),last_day_of_month(begin).strftime("%Y-%m-%d")))
            begin = next_month
        result.append ((begin.strftime("%Y-%m-%d"),end.strftime("%Y-%m-%d")))
        return result


    date_list = monthlist(pull_start_date,pull_end_date)

    date_list.reverse()

    print(date_list)


    for date_range in tqdm(
      date_list, desc = "Loading GA3 data"
    ):
      mylist = []
      pageToken = "0"
      df = pd.DataFrame()
      
      while pageToken != None:
          response = get_report(ua, ga3_view_id, date_range[0], date_range[1], pageToken)
          pageToken = get_token(response)
          dict_transfer(response, mylist)

      df = pd.DataFrame(mylist)
      df['ga:landingPagePath'].loc[df['ga:landingPagePath'] != "(not set)"] = 'https://' + df['ga:hostname'].loc[df['ga:landingPagePath'] != "(not set)"] + df['ga:landingPagePath'].loc[df['ga:landingPagePath'] != "(not set)"].astype(str)
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
      goal_metric: 'conversions',
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
