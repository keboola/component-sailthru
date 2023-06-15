"""
Template Component main class.

"""
import csv
import json
import logging
import os
import tempfile
import time
from datetime import datetime
from pathlib import Path
from typing import List, Iterator

from keboola.component.base import ComponentBase
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from sailthru.sailthru_client import SailthruClient

# configuration variables
from configuration import Configuration, LoadMode
from json_converter import JsonConverter

KEY_API_TOKEN = '#api_token'
KEY_PRINT_HELLO = 'print_hello'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_PRINT_HELLO]
REQUIRED_IMAGE_PARS = []


class LogWriter:
    def __init__(self, log_table: TableDefinition):
        self.log_table = log_table
        os.makedirs(Path(log_table.full_path).parent, exist_ok=True)
        self._out_stream = open(log_table.full_path, 'w+')
        self._writer = csv.DictWriter(self._out_stream, fieldnames=['row_id', 'status', 'detail', 'timestamp'])
        self._writer.writeheader()

    def _build_pk_hash(self, pkey: List) -> str:
        pkey_str = [str(key) for key in pkey]
        return '|'.join(pkey_str)

    def write_record_single(self, row_id: dict, status: str, detail: str):
        row = {"row_id": row_id,
               "status": status,
               "detail": detail,
               "timestamp": datetime.utcnow().isoformat()}
        self._writer.writerow(row)

    def close(self):
        self._out_stream.close()


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self._configuration: Configuration
        self.client: SailthruClient

    def run(self):
        """
        Main execution code
        """

        logging.info('Processing input mapping.')

        in_tables = self.get_input_tables_definitions()
        self._init_configuration()
        self._init_client()

        if len(in_tables) == 0:
            logging.exception('There is no table specified on the input mapping! You must provide one input table!')
            exit(1)
        elif len(in_tables) > 1:
            logging.warning(
                'There is more than one table specified on the input mapping! You must provide one input table!')

        in_table = in_tables[0]
        json_params = self._configuration.json_mapping
        converter = JsonConverter(nesting_delimiter=json_params.nesting_delimiter,
                                  infer_data_types=json_params.column_data_types.autodetect,
                                  column_data_types=json_params.column_data_types.datatype_override)

        log_out = self.create_out_table_definition('result_log.csv', incremental=False,
                                                   primary_key=['row_id', 'status'])
        log_writer = LogWriter(log_out)
        success = True
        with open(in_table.full_path, 'r') as inp:
            reader = csv.reader(inp)

            if self._configuration.destination.mode == LoadMode.users_bulk:
                success = self._load_bulk(in_table, converter, reader, log_writer)

            elif self._configuration.destination.mode == LoadMode.endpoint:
                success = self._load_direct(in_table, converter, reader, log_writer)

        log_writer.close()
        manifest = log_out.get_manifest_dictionary()

        if 'queuev2' in os.environ.get('KBC_PROJECT_FEATURE_GATES', ''):
            manifest['write_always'] = True
        else:
            logging.warning("Running on old queue, "
                            "result log will not be stored unless continue on failure is selected")
        with open(log_out.full_path + '.manifest', 'w') as manifest_file:
            json.dump(manifest, manifest_file)

        if not success:
            raise UserException("Execution failed with errors. See the result log table for details.")

    def _wait_until_job_finished(self, job_id):
        counter = 0
        while True:  # TODO: consider some timeout - currently we terminate on 'DONE' or abort on error
            counter += 1
            if counter % 4 == 0:
                logging.info("Job is pending, waiting for the job to finish.")
            response = self.client.api_get('job', {"job_id": job_id})
            if not response.is_ok():
                err_message = response.get_error().get_message()
                raise UserException(f"Checking job failed with error: {err_message}")

            body = response.get_body()
            state = body['status']
            logging.info(f"Checking report state : {state}")

            if state == 'completed':
                return True
            if state == 'error':
                logging.error(f'The bulk JOB ({job_id}) failed: {body}')
                return False

            time.sleep(30)

    def _load_bulk(self, in_table: TableDefinition, converter: JsonConverter, input_data: Iterator,
                   log_writer: LogWriter):
        tmp_file, file_name = tempfile.mkstemp()
        with open(file_name, 'w+') as tmp_inp:
            for json_data in converter.convert_stream(in_table.columns, input_data):
                json_data.pop('row_id')
                tmp_inp.write(json.dumps(json_data))
                tmp_inp.write('\n')

        response = self.client.api_post("job", {}, {"file": file_name})

        if not response.is_ok():
            err_message = response.get_error().get_message()
            raise UserException(f"Bulk job failed with error: {err_message}")

        body = response.get_body()
        logging.info(f"Job ID: {body['job_id']} submitted successfully!")
        return self._wait_until_job_finished(body['job_id'])

    def _load_direct(self, in_table: TableDefinition, converter: JsonConverter, input_data: Iterator,
                     log_writer: LogWriter):
        failed = False
        for json_data in converter.convert_stream(in_table.columns, input_data):
            row_id = json_data.pop('row_id')
            if self._configuration.destination.method == 'POST':
                response = self.client.api_post(self._configuration.destination.endpoint, json_data)
                if not response.is_ok():
                    failed = True
                    err_message = response.get_error().get_message()
                    log_writer.write_record_single(row_id, 'error', err_message)
                else:
                    log_writer.write_record_single(row_id, 'success', '')
        return failed

    def _init_configuration(self) -> None:
        self.validate_configuration_parameters(Configuration.get_dataclass_required_parameters())
        self._configuration: Configuration = Configuration.load_from_dict(self.configuration.parameters)

    def _init_client(self) -> None:
        self.client = SailthruClient(self._configuration.pswd_api_key, self._configuration.pswd_secret)

    def test_connection(self):
        self._init_configuration()
        self._init_client()
        self.client.api_get('settings')


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
