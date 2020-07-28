import json
from absl import logging


class edsmObject:
  def __init__(self, file_type: str, json_string: str):
    self.file_type = file_type
    self.json_string = json_string

  def _format_powerplay(self):
    input_object = json.loads(self.json_string)
    output_object = {
        'id': input_object.get('id64'),
        'state': input_object.get('state'),
        'power': {
            'name': input_object.get('power'),
            'state': input_object.get('powerState'),
        },
        'allegiance': input_object.get('allegiance'),
        'government': input_object.get('government'),
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def _format_systems(self):
    input_object = json.loads(self.json_string)
    output_object = {
        'id': input_object.get('id64'),
        'name': input_object.get('name'),
        'coordinates': {
            'x': input_object.get('coords', {}).get('x'),
            'y': input_object.get('coords', {}).get('y'),
            'z': input_object.get('coords', {}).get('z'),
            'coordinates': '%s, %s, %s' % (
                input_object.get('coords', {}).get('x'),
                input_object.get('coords', {}).get('y'),
                input_object.get('coords', {}).get('z'),
            ),
        },
        'updated': input_object.get('date'),
    }

    return json.dumps(output_object)

  def format_json(self):
    if self.file_type == 'powerplay':
      formatted_json = self._format_powerplay()
    elif self.file_type == 'systems':
      formatted_json = self._format_systems()
    else:
      logging.error('Unsupported input file type.')
      formatted_json = None

    return formatted_json
