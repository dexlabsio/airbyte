#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_dex_meta_marketing_api import SourceDexMetaMarketingApi

if __name__ == "__main__":
    source = SourceDexMetaMarketingApi()
    launch(source, sys.argv[1:])
