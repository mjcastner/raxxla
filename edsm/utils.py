import json
import re

from absl import logging

# Global vars
JSON_RE_PATTERN = re.compile(r'(\{.*\})')
JSON_RE_SEARCH = JSON_RE_PATTERN.search

def extract_json(raw_input):
  json_re_match = JSON_RE_SEARCH(raw_input)
  if json_re_match:
    json_string = json_re_match.group(1)
    return json_string
