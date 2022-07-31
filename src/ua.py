import os
import datetime
import re
from typing import Union, List, Tuple

import googleapiclient.discovery
import google.auth




class UniversalAnalytics:
    def __init__(self, service_account_file: str,
                       project_name: str = None):
        self.service_account_file = service_account_file
        self.project_name = project_name

    @property
    def service(self) -> googleapiclient.discovery.Resource:
        """Builds the discovery document for Universal Analytics.

        This is a simple facade for the Google API client discovery builder. For
        full details, refer to the following documentation:
        https://googleapis.github.io/google-api-python-client/docs/epy/googleapiclient.discovery-module.html#build
        """
        return googleapiclient.discovery.build(
            'analyticsreporting',
            'v4',
            credentials=self.get_service_account_credentials(),
            cache_discovery=False)


    def get_service_account_credentials(self):

        SCOPES = [
            "https://www.googleapis.com/auth/analytics.readonly",
          ]
        cred_kwargs = {"scopes": SCOPES}
        if self.project_name:
          cred_kwargs["quota_project_id"] = self.project_name

        credentials, _ = google.auth.load_credentials_from_file( self.service_account_file or os.environ.get('GOOGLE_APPLICATION_CREDENTIALS'),
                        **cred_kwargs
                      )
        return credentials

    def query(self, view_id: str) -> 'AnalyticsQuery':
        return AnalyticsQuery(self, view_id)


class AnalyticsQuery:
    def __init__(self, ua: UniversalAnalytics, view_id: str):
        self.ua = ua
        self.raw = {
            'reportRequests': [{
                'viewId': view_id,
                'dateRanges': [{
                    'startDate': self.iso_date(self.days_ago(91)),
                    'endDate': self.iso_date(self.days_ago(1))
                }],
                'metrics': [{'expression': 'ga:users'}],
                'samplingLevel': 'LARGE'
            }]
        }

    @staticmethod
    def iso_date(date: Union[str, datetime.date]) -> str:
        if isinstance(date, datetime.date):
            date = date.isoformat()
        iso_regex = r'^\d{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[1-2][0-9]|3[0-1])$'
        if not re.fullmatch(iso_regex, date):
            raise ValueError(f'The specified date is not in valid ISO format '
                             f'"YYYY-MM-DD": "{date}".')
        return date

    @staticmethod
    def days_ago(days: int) -> datetime.date:
        today = datetime.date.today()
        return today - datetime.timedelta(days=abs(days))

    def date_range(
        self,
        date_ranges: List[Tuple[Union[str, datetime.date],
                                Union[str, datetime.date]]]
    ) -> 'AnalyticsQuery':
        """Return a new query for metrics within a given date range.

        Args:
            date_ranges:
                A list of tuples of date ranges for which to query the report.
                Each tuple must contain two date strings in ISO format
                (e.g. '2022-04-22').

        Returns:
            An updated AnalyticsQuery object.
        """
        query_date_ranges = []
        for dr in date_ranges:
            start_date, end_date = dr
            start_date = self.iso_date(start_date)
            end_date = self.iso_date(end_date)
            query_date_ranges.append({
                'startDate': start_date,
                'endDate': end_date
            })
        self.raw['reportRequests'][0]['dateRanges'] = query_date_ranges

        return self

    def dimensions(self, dimensions: List[str]) -> 'AnalyticsQuery':
        """Return a new query that fetches the specified dimensions.

        Args:
            dimensions:
                Dimensions you would like to report on. The name of the
                dimension must start with 'ga:'. Refer to the following
                documentation for the full list of available dimensions:
                https://ga-dev-tools.web.app/dimensions-metrics-explorer/

        Returns:
            An updated AnalyticsQuery object.
        """
        self.raw['reportRequests'][0]['dimensions'] = [{'name': dim} for dim in dimensions]
        return self

    def metrics(self, metrics: List[str]) -> 'AnalyticsQuery':
        """Return a new query that fetches the specified metrics.

        Args:
            metrics:
                Metrics you would like to report on. The name of the metric
                must start with 'ga:'. Refer to the following documentation
                for the full list of available metrics:
                https://ga-dev-tools.web.app/dimensions-metrics-explorer/

        Returns:
            An updated AnalyticsQuery object.
        """
        self.raw['reportRequests'][0]['metrics'] = [{'expression': metric} for metric in metrics]
        return self

    def segment(self, segments: List[str]) -> 'AnalyticsQuery':
        """Return a new query that fetches with the specified segments.

        Args:
            segments:
                Segments you would like to report with. The ID of the segment
                must start with 'gaid::'. You can use the following tool to
                identify segment IDs:
                https://ga-dev-tools.web.app/query-explorer/

        Returns:
            An updated AnalyticsQuery object.
        """
        self.raw['reportRequests'][0]['segments'] = [{'segmentId': seg} for seg in segments]
        return self

    def page_size(self, page_size: int) -> 'AnalyticsQuery':
        """Return a new query that fetches with the specified segments.

        Args:
            page_size:
                Specifies the maximum number of returned rows for a query.
                Returns a maximum of 100,000 rows per request, no matter how
                many you ask for.

        Returns:
            An updated AnalyticsQuery object.
        """
        self.raw['reportRequests'][0]['pageSize'] = page_size
        return self

    def page_token(self, page_token: str) -> 'AnalyticsQuery':
        """Return a new query that fetches with the specified segments.

        Args:
            page_token:
                A continuation token to get the next page of results.

        Returns:
            An updated AnalyticsQuery object.
        """
        self.raw['reportRequests'][0]['pageToken'] = page_token
        return self

    def sampling_level(self, sampling_level: str) -> 'AnalyticsQuery':
        """Return a new query that fetches with the specified segments.

        Args:
            sampling_level:
                Field to set desired sample size.

        Returns:
            An updated AnalyticsQuery object.
        """
        self.raw['reportRequests'][0]['samplingLevel'] = sampling_level
        return self

    def get(self) -> 'AnalyticsReport':
        raw_report = self.ua.service.reports().batchGet(body=self.raw).execute()
        return AnalyticsReport(raw_report, self)


class AnalyticsReport:
    def __init__(self, raw: List[dict], query: AnalyticsQuery):
        self.raw = raw
        self.query = query
