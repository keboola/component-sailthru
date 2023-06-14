import logging
from typing import List, Dict, Optional, Generator

from csv2json.hone_csv2json import Csv2JsonConverter


class JsonConverter:

    def __init__(self, nesting_delimiter: str = '__',
                 infer_data_types=True,
                 column_data_types: Optional[List[Dict[str, str]]] = None,
                 column_name_override: Optional[dict] = None):

        self.nesting_delimiter = nesting_delimiter
        self.infer_data_types = infer_data_types
        self.column_data_types = column_data_types or []
        self.column_name_override = column_name_override or {}

    def convert_stream(self, header, reader) -> Generator[dict, None, None]:
        converter = Csv2JsonConverter(header, delimiter=self.nesting_delimiter)
        # fetch first row
        row = next(reader, None)

        if not row:
            logging.warning('The file is empty!')

        for row in reader:
            result = converter.convert_row(row=row,
                                           coltypes=self.column_data_types,
                                           delimit=self.nesting_delimiter,
                                           colname_override=self.column_name_override,
                                           infer_undefined=self.infer_data_types)

            yield result[0]
