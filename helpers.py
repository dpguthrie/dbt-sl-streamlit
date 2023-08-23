# stdlib
import base64
from typing import Dict, List

# third party
import pyarrow as pa


def keys_exist_in_dict(keys_list, dct):
    return all(key in dct for key in keys_list)


def get_shared_elements(all_elements: List[List]):
    if len(all_elements) == 0:
        return []
    
    try:
        unique = set(all_elements[0]).intersection(*all_elements[1:])
    except IndexError:
        unique = set(all_elements[0])
        
    return list(unique)


def to_arrow_table(byte_string: str, to_pandas: bool = True) -> pa.Table:
    with pa.ipc.open_stream(base64.b64decode(byte_string)) as reader:
        arrow_table = pa.Table.from_batches(reader, reader.schema)
        
    if to_pandas:
        return arrow_table.to_pandas()
    
    return arrow_table
