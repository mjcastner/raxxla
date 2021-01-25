from datetime import datetime
import functools


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
    for k, v in schema_mapping.items():
        if k == 'timestamp_fields':
            for ts_k, ts_v in v.items():
                int_ts = int(
                    datetime.strptime(recursive_dict_get(input_dict, ts_v),
                                      '%Y-%m-%d %H:%M:%S').timestamp())
                recursive_class_set(proto_obj, ts_k, int_ts)
        elif k == 'repeated_fields':
            pass
        else:
            dict_value = recursive_dict_get(input_dict, v)
            if dict_value:
                recursive_class_set(proto_obj, k, dict_value)
