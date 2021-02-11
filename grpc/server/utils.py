from datetime import datetime
import functools
import re

import bodies_pb2

JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search
PART_NAME_RE = re.compile(r'[0-9]{1}[A-Z]{1}.{1}(.*)')
PART_QUALITY_RE = re.compile(r'[0-9]{1}[A-Z]{1}')


def extract_json(raw_input: str):
    json_re_match = JSON_RE_SEARCH(raw_input)
    if json_re_match:
        json_string = json_re_match.group(1)
        return json_string

def extract_quality(input_string: str):
    name_match = re.search(PART_NAME_RE, input_string)
    quality_match = re.search(PART_QUALITY_RE, input_string)

    return {
        'name': name_match.group(1),
        'quality': quality_match.group(0),
    }


def recursive_class_get(obj, attr, *args):
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split('.'))


def recursive_class_set(obj, attr, val):
    pre, _, post = attr.rpartition('.')
    return setattr(recursive_class_get(obj, pre) if pre else obj, post, val)


def recursive_dict_get(input_dict: dict, path: str):
    return functools.reduce(dict.get, path.split('.'), input_dict)


def recursive_dict_set(input_dict, path, value):
    keys = path.split('.')
    for key in keys[:-1]:
        input_dict = input_dict.setdefault(key, {})
    input_dict[keys[-1]] = value


def map_proto_fields(proto_obj, schema_mapping, input_dict):
    def _format_composition(input_composition: dict):
        output_composition = []
        if input_composition:
            for element, percentage in input_composition.items():
                composition = bodies_pb2.Composition()
                composition.type = element
                composition.percentage = percentage
                output_composition.append(composition)

        return output_composition

    def _format_parents(input_parents: list):
        output_parents = []
        if input_parents:
            for input_parent in input_parents:
                for parent_type, parent_relative_id in input_parent.items():
                    if parent_type and parent_relative_id:
                        parent = bodies_pb2.Parent()
                        parent.type = parent_type
                        parent.relative_id = parent_relative_id
                        output_parents.append(parent)

        return output_parents

    def _format_ringlike(input_rings: list):
        output_rings = []
        if input_rings:
            for input_ring in input_rings:
                ring = bodies_pb2.Ringlike()
                ring.name = input_ring.get('name')
                ring.type = input_ring.get('type')
                ring.mass = input_ring.get('mass')
                ring.inner_radius = input_ring.get('innerRadius')
                ring.outer_radius = input_ring.get('outerRadius')
                output_rings.append(ring)

        return output_rings

    def _process_timestamp(raw_timestamp):
        return int(
            datetime.strptime(raw_timestamp, '%Y-%m-%d %H:%M:%S').timestamp())

    for field, value in schema_mapping.items():
        if field == 'timestamp_fields':
            for ts_k, ts_v in value.items():
                int_ts = _process_timestamp(
                    recursive_dict_get(input_dict, ts_v))
                recursive_class_set(proto_obj, ts_k, int_ts)
        elif field == 'repeated_fields':
            for rf_field, rf_dict in value.items():
                rf_key = rf_dict.get('key')
                rf_type = rf_dict.get('type')
                if rf_type == 'composition':
                    composition_data = _format_composition(
                        recursive_dict_get(input_dict, rf_key))
                    proto_field = recursive_class_get(proto_obj, rf_field)
                    proto_field.extend(composition_data)
                elif rf_type == 'ringlike':
                    ringlike_data = _format_ringlike(
                        recursive_dict_get(input_dict, rf_key))
                    proto_field = recursive_class_get(proto_obj, rf_field)
                    proto_field.extend(ringlike_data)
                elif rf_type == 'parents':
                    parent_data = _format_parents(
                        recursive_dict_get(input_dict, rf_key))
                    proto_field = recursive_class_get(proto_obj, rf_field)
                    proto_field.extend(parent_data)
        else:
            dict_value = recursive_dict_get(input_dict, value)
            if dict_value:
                recursive_class_set(proto_obj, field, dict_value)
