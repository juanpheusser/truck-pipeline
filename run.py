"""
Run IC Logistic Data Pipeline
"""

from pipeline.common.api_connector import ApiConnector
from pipeline.transformers.transformer import Transformer
from pipeline.common.meta_process import Meta
import pandas as pd
import logging
import logging.config
import yaml
import os

def main():

    """
    Entry point for IC Logistic Data Pipeline
    """

    # Parsing YAML file
    config_path = os.path.join('configs', 'pipeline_config.yml')
    config = yaml.safe_load(open(config_path))

    # Configure logging
    log_config = config['logging']
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    # Configure API
    api_config = config['samtech_api']

    # Configure MongoDB
    mongo_config = config["mongodb"]

    # Configure Transformer
    transformer_config = config["transformer"]

    # Datetime for log syncrony
    now = pd.to_datetime("now")

    # Class instances
    apiConnector = ApiConnector(
        now=now,
        data_endpoint=api_config['data_endpoint'],
        token_endpoint=api_config['token_endpoint'],
        grant_type=api_config['grant_type'],
        content_type=api_config['content_type']
    )

    transformer = Transformer(
        now=now,
        dtypes=transformer_config["dtypes"],
        timeWindow=transformer_config["timeWindow"]
        )

    # Pipeline execution
    t1 = pd.to_datetime('now')
    data = apiConnector.get_data_response()
    data = transformer.transformData(data)
    Meta.writeMetaFile(data, now)
    f_data = transformer.filterData(data)

    f_data.to_csv(
        os.path.join('data',
            ''.join([pd.to_datetime('now').strftime('%Y-%m-%dT%H:%M:%S'), '.csv'])))


    t6 = pd.to_datetime('now')

    Meta.updatePlateMetafile(data)

    print('Total Time: ', (t6 - t1).total_seconds())


if __name__ == '__main__':
    main()
