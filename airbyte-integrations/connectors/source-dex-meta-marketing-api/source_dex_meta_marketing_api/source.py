#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import time
import json
from datetime import datetime
from typing import Dict, Generator

from airbyte_cdk.logger import AirbyteLogger
from airbyte_cdk.models import (
    AirbyteCatalog,
    AirbyteConnectionStatus,
    AirbyteMessage,
    AirbyteRecordMessage,
    AirbyteStream,
    ConfiguredAirbyteCatalog,
    Status,
    Type,
)
from airbyte_cdk.sources import Source

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

MAX_ATTEMPTS = 10


class SourceDexMetaMarketingApi(Source):

    def _manage_api_limits(self, req_object, logger):

        ro_headers = req_object.headers()

        usage_info = json.loads(
            ro_headers.get("x-business-use-case-usage", json.dumps({}))
        )

        # usage_info = next(iter(usage_info.values()))[0]
        if usage_info != {}:
            usage_info = usage_info[list(usage_info.keys())[0]][0]

        throttle_info = json.loads(
            ro_headers.get("x-fb-ads-insights-throttle", json.dumps({}))
        )

        logger.info(f"Usage Info: {usage_info}")
        # logger.info(f'{ro_headers.get("x-fb-insights-stability-throttle")}')
        logger.info(f"Throttle Info: {throttle_info}")

        if max(
            throttle_info.get("app_id_util_pct", 0),
            throttle_info.get("acc_id_util_pct", 0),
            usage_info.get('call_count', 0),
            usage_info.get('total_cputime', 0),
            usage_info.get('total_time', 0)
        ) > 90:
            logger.info("Close to rate limit. Sleeping 3 minutes")
            time.sleep(3*60)

    def _get_api(self, config: json) -> FacebookAdsApi:

        return FacebookAdsApi.init(
            app_id=config.get('app_id'),
            access_token=config.get('access_token'),
            api_version="v17.0"
        )

    def check(
        self,
        logger: AirbyteLogger,
        config: json
    ) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully
        connect to the integration
            e.g: if a provided Stripe API token can be used to connect
            to the Stripe API.

        :param logger: Logging object to display debug/info/error to the logs
        :param config: Json object containing the configuration of this source,
        content of this json is as specified in the properties of the
        spec.yaml file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        logger.info("Calling check")
        try:
            self._get_api(config)

            ad_account = AdAccount(config.get('account_id'))

            insights = ad_account.get_insights()

            self._manage_api_limits(insights, logger)

            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(
                status=Status.FAILED,
                message=f"An exception occurred: {str(e)}"
            )

    def discover(self, logger: AirbyteLogger, config: json) -> AirbyteCatalog:
        """
        Returns an AirbyteCatalog representing the available streams and fields
        in this integration.
        For example, given valid credentials to a Postgres database, returns an
        Airbyte catalog where each postgres table is a stream, and each table
        column is a field.

        :param logger: Logging object to display debug/info/error to the logs
        :param config: Json object containing the configuration of this source,
        content of this json is as specified in the properties of the
        spec.yaml file

        :return: AirbyteCatalog is an object describing a list of all available
        streams in this source.
            A stream is an AirbyteStream object that includes:
            - its stream name (or table name in the case of Postgres)
            - json_schema providing the specifications of expected schema for
            this stream (a list of columns described by their names and types)
        """
        logger.info("Calling discover")
        self._get_api(config)

        ad_account = AdAccount(config.get('account_id'))

        campaigns = ad_account.get_campaigns()

        self._manage_api_limits(campaigns, logger)

        streams = []

        for camp in campaigns:

            insights = camp.get_insights(fields=['campaign_name'])

            self._manage_api_limits(insights, logger)

            if len(insights) == 0:
                continue

            stream_name = insights[0]['campaign_name']

            # adsets = camp.get_ad_sets()

            # adset_names = []

            # for adset in adsets:

            #     adset_insights = adset.get_insights(fields=['adset_name'])

            #     if len(adset_insights) == 0:
            #         continue

            #     adset_names.append(adset_insights[0]['adset_name'])

            # properties = [
            #     {adset_name: {"type": "string"}}
            #     for adset_name in adset_names
            # ]

            json_schema = {
                "$schema": "http://json-schema.org/draft-07/schema#",
                "type": "object",
            }

            supported_sync_modes = ['full_refresh']
            source_defined_cursor = False
            default_cursor_field = None

            streams.append(
                AirbyteStream(
                    name=stream_name,
                    json_schema=json_schema,
                    supported_sync_modes=supported_sync_modes,
                    source_defined_cursor=source_defined_cursor,
                    default_cursor_field=default_cursor_field
                )
            )

        return AirbyteCatalog(streams=streams)

    def read(
        self,
        logger: AirbyteLogger,
        config: json,
        catalog: ConfiguredAirbyteCatalog,
        state: Dict[str, any]
    ) -> Generator[AirbyteMessage, None, None]:
        """
        Returns a generator of the AirbyteMessages generated by reading the
        source with the given configuration, catalog, and state.

        :param logger: Logging object to display debug/info/error to the logs
        :param config: Json object containing the configuration of this source,
        content of this json is as specified in the properties of the
        spec.yaml file
        :param catalog: The input catalog is a ConfiguredAirbyteCatalog which
        is almost the same as AirbyteCatalog returned by discover(), but in
        addition, it's been configured in the UI! For each particular stream
        and field, there may have been provided with extra modifications such
        as: filtering streams and/or columns out, renaming some entities, etc
        :param state: When a Airbyte reads data from a source, it might need
        to keep a checkpoint cursor to resume replication in the future from
        that saved checkpoint. This is the object that is provided with state
        from previous runs and avoid replicating the entire set of data
        everytime.

        :return: A generator that produces a stream of AirbyteRecordMessage
        contained in AirbyteMessage object.
        """
        logger.info("Calling read")
        filt_params = {
            'limit': 1000,
            'date_preset': 'last_90d',
            'filtering': [
                {
                    'field': "impressions",
                    'operator': "GREATER_THAN",
                    'value': 0
                }
            ]
        }

        params = {
            'limit': 100,
            'date_preset': 'last_90d',
            'filtering': [
                {
                    'field': "impressions",
                    'operator': "GREATER_THAN",
                    'value': 0
                }
            ],
            'time_increment': 1,
        }

        self._get_api(config)

        ad_account = AdAccount(config.get('account_id'))

        object_type = config.get('object_type') or 'campaign'

        function_mapping = {
            'adset': ad_account.get_ad_sets,
            'campaign': ad_account.get_campaigns,
            'ad': ad_account.get_ads
        }

        params['level'] = object_type

        func = function_mapping[object_type]

        logger.info(f"Reading all {object_type}s in ad account")

        ad_objects = func(params=filt_params)

        self._manage_api_limits(ad_objects, logger)

        ad_objects_list = []

        for obj in ad_objects:

            while True:
                try:
                    ad_objects_list.append(obj)
                    break
                except Exception as e:
                    logger.info(f"Exception caught getting ad objects: {e}")
                    logger.info("Sleeping 5 minutes")
                    time.sleep(5*60)

        params['breakdowns'] = config.get('breakdowns')

        for ad_obj in ad_objects_list:

            n_attempts = MAX_ATTEMPTS

            while n_attempts > 0:
                try:
                    ad_obj_id = ad_obj['id']

                    logger.info(f"Processing {object_type} - {ad_obj_id} (attempt {MAX_ATTEMPTS - n_attempts + 1})")

                    insights = ad_obj.get_insights(
                        params=params,
                        # fields=fields
                    )

                    self._manage_api_limits(insights, logger)

                    if len(insights) > 0:
                        stream_name = f"{object_type}_{ad_obj_id}"

                        for ins in insights:
                            yield AirbyteMessage(
                                type=Type.RECORD,
                                record=AirbyteRecordMessage(
                                    stream=stream_name,
                                    data=ins,
                                    emitted_at=int(
                                        datetime.now().timestamp()
                                        )*1000
                                ),
                            )

                    break
                except Exception as e:

                    logger.info("Caught Exception")
                    logger.info(e)
                    logger.info("Sleeping 15 minutes")
                    time.sleep(15*60)
                    if n_attempts == 1:
                        logger.info(f"Object {object_type} - {ad_obj_id} could not be ingested. Skipping")
                    n_attempts -= 1
