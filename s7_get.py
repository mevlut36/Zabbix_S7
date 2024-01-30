#!/usr/bin/env python3

import argparse
import json
import ipaddress
import snap7

def create_parser():
    helptext = """S7 for Zabbix script. Requires snap7 and python-snap7 wrapper."""
    parser = argparse.ArgumentParser(description=helptext)
    parser.add_argument("ip_address", type=ipaddress.ip_address)
    parser.add_argument("rack", type=int)
    parser.add_argument("slot", type=int)
    parser.add_argument("DB", type=int)
    parser.add_argument("offset", type=str, help="For bool, format as '6.1' for bit 1 of byte 6")
    parser.add_argument("datatype", choices=['int', 'float', 'bool', 'string'])
    parser.add_argument("bytes_to_read", type=int, nargs="?")
    parser.add_argument("--json", action='store_true')
    return parser.parse_args()

def connect_to_plc(ip, rack, slot):
    plc = snap7.client.Client()
    plc.connect(str(ip), rack, slot)
    return plc

def disconnect_from_plc(plc):
    plc.disconnect()

def read_data_from_plc(plc, db, offset, bytes_to_read):
    return plc.db_read(db, offset, bytes_to_read)

def parse_offset(offset):
    if '.' in offset:
        offset, bit_index = map(int, offset.split(".", 2))
        return offset, [bit_index]
    return int(offset), range(8)

def process_data(bytes_response, datatype, bit_indexes=None, bytes_to_read=None):
    if datatype == 'int':
        return [str(snap7.util.get_int(bytes_response, 0))]
    elif datatype == 'float':
        return [str(snap7.util.get_real(bytes_response, 0))]
    elif datatype == 'bool':
        return [str(snap7.util.get_bool(bytes_response, 0, i)) for i in bit_indexes]
    elif datatype == 'string':
        if bytes_to_read is None:
            raise ValueError("bytes_to_read must be specified for string datatype")
        max_length = snap7.util.get_int(bytes_response, 0)
        return [snap7.util.get_string(bytes_response, 1, min(bytes_to_read, max_length))]


if __name__ == "__main__":
    args = create_parser()

    if args.datatype == 'string':
        raise NotImplementedError("String datatype not implemented yet.")

    offset, bit_indexes = parse_offset(args.offset)
    bytes_to_read = args.bytes_to_read or 2 if args.datatype == 'int' else 4 if args.datatype == 'float' else 1

    plc = connect_to_plc(args.ip_address, args.rack, args.slot)
    try:
        bytes_response = read_data_from_plc(plc, args.DB, offset, bytes_to_read)
    finally:
        disconnect_from_plc(plc)

    response = process_data(bytes_response, args.datatype, bit_indexes)

    if args.json:
        print(json.dumps({str(offset): response}))
    else:
        print(" ".join(response))
