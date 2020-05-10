import json

class system:
  def __init__(self):
    self.id = None
    self.name = None
    self.date = None
    self.coordinates = {
      'x': None,
      'y': None,
      'z': None,
      'coordinates': None
    }

  def from_json(self, input_json):
    system_data = json.loads(input_json)
    
    self.id = system_data['id']
    self.name = system_data['name']
    self.date = system_data['date']
    self.coordinates['x'] = system_data['coords']['x']
    self.coordinates['y'] = system_data['coords']['y']
    self.coordinates['z'] = system_data['coords']['z']
    self.coordinates['coordinates'] = '%s, %s, %s' % (self.coordinates['x'],
                                                      self.coordinates['y'],
                                                      self.coordinates['z'])

  def to_json(self):
    return json.dumps(self, 
                      default=lambda 
                      o: o.__dict__)
