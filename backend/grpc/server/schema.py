import functools
import json
import re
from datetime import datetime

import bodies_pb2
import society_pb2
import settlement_pb2
import system_pb2
from absl import logging
from commonlib import utils


class EdsmObject:
    def __init__(self, file_type, json_string):
        self.file_type = file_type
        self.json_string = json_string
        self.json_dict = None
        self.part_name_re = re.compile(r'[0-9]{1}[A-Z]{1}.{1}(.*)')
        self.part_quality_re = re.compile(r'[0-9]{1}[A-Z]{1}')
        self.schema_mappings = {
            'planet': {
                'id': 'id64',
                'system_id': 'systemId64',
                'relative_id': 'bodyId',
                'name': 'name',
                'atmosphere.type': 'atmosphereType',
                'metadata.type': 'subType',
                'metadata.distance': 'distanceToArrival',
                'metadata.mass': 'earthMasses',
                'metadata.gravity': 'gravity',
                'metadata.landable': 'isLandable',
                'metadata.radius': 'radius',
                'metadata.temperature': 'surfaceTemperature',
                'metadata.pressure': 'surfacePressure',
                'metadata.volcanism': 'volcanismType',
                'metadata.terraforming': 'terraformingState',
                'orbit.period': 'orbitalPeriod',
                'orbit.rotational_period': 'rotationalPeriod',
                'orbit.tidally_locked': 'rotationalPeriodTidallyLocked',
                'orbit.periapsis': 'argOfPeriapsis',
                'orbit.eccentricity': 'orbitalEccentricity',
                'orbit.inclination': 'orbitalInclination',
                'orbit.semimajor_axis': 'semiMajorAxis',
                'orbit.axial_tilt': 'axialTilt',
                'repeated_fields': {
                    'atmosphere.composition': {
                        'key': 'atmosphereComposition',
                        'type': 'composition',
                    },
                    'belts': {
                        'key': 'belts',
                        'type': 'ringlike',
                    },
                    'composition': {
                        'key': 'solidComposition',
                        'type': 'composition',
                    },
                    'materials': {
                        'key': 'materials',
                        'type': 'composition',
                    },
                    'parents': {
                        'key': 'parents',
                        'type': 'parents',
                    },
                    'rings': {
                        'key': 'rings',
                        'type': 'ringlike',
                    },
                },
                'timestamp_fields': {
                    'updated': 'updateTime',
                },
            },
            'population': {
                'system_id': 'id64',
                'allegiance': 'allegiance',
                'government': 'government',
                'state': 'state',
                'economy': 'economy',
                'security': 'security',
                'population': 'population',
                'repeated_fields': {
                    'factions': {
                        'key': 'factions',
                        'type': 'factions',
                    },
                },
                'timestamp_fields': {
                    'updated': 'date',
                },
            },
            'powerplay': {
                'system_id': 'id64',
                'power.name': 'power',
                'power.state': 'powerState',
                'allegiance': 'allegiance',
                'government': 'government',
                'state': 'state',
                'timestamp_fields': {
                    'updated': 'date',
                },
            },
            'star': {
                'id': 'id64',
                'system_id': 'systemId64',
                'relative_id': 'bodyId',
                'name': 'name',
                'metadata.type': 'subType',
                'metadata.distance': 'distanceToArrival',
                'metadata.reserve_level': 'reserveLevel',
                'metadata.spectral_class': 'spectralClass',
                'metadata.solar_masses': 'solarMasses',
                'metadata.solar_radius': 'solarRadius',
                'metadata.luminosity': 'luminosity',
                'metadata.temperature': 'surfaceTemperature',
                'orbit.period': 'orbitalPeriod',
                'orbit.rotational_period': 'rotationalPeriod',
                'orbit.tidally_locked': 'rotationalPeriodTidallyLocked',
                'orbit.periapsis': 'argOfPeriapsis',
                'orbit.eccentricity': 'orbitalEccentricity',
                'orbit.inclination': 'orbitalInclination',
                'orbit.semimajor_axis': 'semiMajorAxis',
                'orbit.axial_tilt': 'axialTilt',
                'repeated_fields': {
                    'belts': {
                        'key': 'belts',
                        'type': 'ringlike',
                    },
                    'parents': {
                        'key': 'parents',
                        'type': 'parents',
                    },
                    'rings': {
                        'key': 'rings',
                        'type': 'ringlike',
                    },
                },
                'timestamp_fields': {
                    'updated': 'updateTime',
                },
            },
            'stations': {
                'id': 'id',
                'system_id': 'systemId64',
                'name': 'name',
                'metadata.type': 'type',
                'metadata.distance': 'distanceToArrival',
                'metadata.allegiance': 'allegiance',
                'metadata.controlling_faction': 'controllingFaction.id',
                'metadata.government': 'government',
                'economy.id': 'marketId',
                'economy.type': 'economy',
                'economy.sub_type': 'secondEconomy',
                'services.market': 'haveMarket',
                'services.shipyard': 'haveShipyard',
                'services.outfitting': 'haveOutfitting',
                'repeated_fields': {
                    'services.commodities': {
                        'key': 'commodities',
                        'type': 'commodities',
                    },
                    'services.other': {
                        'key': 'otherServices',
                        'type': 'string',
                    },
                    'services.ships': {
                        'key': 'ships',
                        'type': 'ships',
                    },
                    'services.ship_parts': {
                        'key': 'outfitting',
                        'type': 'ship_parts',
                    },
                },
                'timestamp_fields': {
                    'updated': 'updateTime.information',
                },
            },
            'system': {
                'id': 'id64',
                'name': 'name',
                'coordinates.x': 'coords.x',
                'coordinates.y': 'coords.y',
                'coordinates.z': 'coords.z',
                'timestamp_fields': {
                    'timestamp': 'date',
                },
            },
        }

        try:
            self.json_dict = json.loads(utils.extract_json(self.json_string))
        except Exception as e:
            logging.error(e)

    def _extract_quality(self, input_string: str):
        name_match = re.search(self.part_name_re, input_string)
        quality_match = re.search(self.part_quality_re, input_string)

        return {
            'name': name_match.group(1),
            'quality': quality_match.group(0),
        }

    def _map_proto_fields(self, proto_obj, schema_mapping, input_dict):
        def _format_composition(input_composition: dict):
            output_composition = []
            if input_composition:
                for element, percentage in input_composition.items():
                    composition = bodies_pb2.Composition()
                    composition.type = element
                    composition.percentage = percentage
                    output_composition.append(composition)

            return output_composition

        def _format_commodities(input_commodities: list):
            output_commodities = []
            if input_commodities:
                for input_commodity in input_commodities:
                    commodity = settlement_pb2.Commodity()
                    commodity.name = input_commodity.get('name')
                    commodity.buy_price = input_commodity.get('buyPrice')
                    commodity.sell_price = input_commodity.get('sellPrice')
                    commodity.stock = input_commodity.get('stock')
                    commodity.demand = input_commodity.get('demand')
                    output_commodities.append(commodity)
            return output_commodities

        def _format_factions(input_factions: list):
            output_factions = []
            if input_factions:
                for input_faction in input_factions:
                    faction = society_pb2.Faction()
                    faction.id = input_faction.get('id')
                    faction.name = input_faction.get('name')
                    faction.allegiance = input_faction.get('allegiance')
                    faction.government = input_faction.get('government')
                    faction.influence = input_faction.get('influence')
                    faction.happiness = input_faction.get('happiness')
                    faction.player_faction = input_faction.get('isPlayer')

                    active_states = _format_states(
                        'active', input_faction.get('activeStates'))
                    faction.states.extend(active_states)

                    pending_states = _format_states(
                        'pending', input_faction.get('pendingStates'))
                    faction.states.extend(pending_states)

                    recovering_states = _format_states(
                        'recovering', input_faction.get('recoveringStates'))
                    faction.states.extend(recovering_states)

                    output_factions.append(faction)

            return output_factions

        def _format_parents(input_parents: list):
            output_parents = []
            if input_parents:
                for input_parent in input_parents:
                    for parent_type, parent_relative_id in input_parent.items(
                    ):
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

        def _format_ship_parts(input_parts: list):
            output_parts = []
            if input_parts:
                for input_part in input_parts:
                    ship_part = settlement_pb2.ShipPart()
                    ship_data = self._extract_quality(input_part.get('name'))
                    ship_part.name = ship_data.get('name')
                    ship_part.quality = ship_data.get('quality')
                    output_parts.append(ship_part)
            return output_parts

        def _format_ships(input_ships: list):
            if input_ships:
                return [x.get('name') for x in input_ships]

        def _format_states(state_type: str, input_states: list):
            output_states = []
            for input_state in input_states:
                state = society_pb2.State()
                state.type = state_type
                state.name = input_state.get('state')
                output_states.append(state)
            return output_states

        def _process_timestamp(raw_timestamp):
            if raw_timestamp:
                return int(
                    datetime.strptime(raw_timestamp,
                                      '%Y-%m-%d %H:%M:%S').timestamp())
            else:
                return 0

        for field, value in schema_mapping.items():
            if field == 'timestamp_fields':
                for ts_k, ts_v in value.items():
                    int_ts = _process_timestamp(
                        utils.recursive_dict_get(input_dict, ts_v))
                    utils.recursive_class_set(proto_obj, ts_k, int_ts)
            elif field == 'repeated_fields':
                for rf_field, rf_dict in value.items():
                    rf_key = rf_dict.get('key')
                    rf_type = rf_dict.get('type')
                    if rf_type == 'composition':
                        composition_data = _format_composition(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(composition_data)
                    elif rf_type == 'ringlike':
                        ringlike_data = _format_ringlike(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(ringlike_data)
                    elif rf_type == 'parents':
                        parent_data = _format_parents(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(parent_data)
                    elif rf_type == 'factions':
                        faction_data = _format_factions(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(faction_data)
                    elif rf_type == 'ships':
                        ship_data = _format_ships(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(ship_data)
                    elif rf_type == 'string':
                        string_data = utils.recursive_dict_get(input_dict, rf_key)
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(string_data)
                    elif rf_type == 'commodities':
                        commodities_data = _format_commodities(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(commodities_data)
                    elif rf_type == 'ship_parts':
                        ship_parts_data = _format_ship_parts(
                            utils.recursive_dict_get(input_dict, rf_key))
                        proto_field = utils.recursive_class_get(
                            proto_obj, rf_field)
                        proto_field.extend(ship_parts_data)
                    else:
                        raise KeyError
            else:
                dict_value = utils.recursive_dict_get(input_dict, value)
                if dict_value:
                    utils.recursive_class_set(proto_obj, field, dict_value)

    def generate_proto(self):
        if self.file_type == 'bodies':
            body_type = self.json_dict.get('type')
            schema = self.schema_mappings.get(body_type.lower())
            if body_type == 'Planet':
                proto_obj = bodies_pb2.Planet()
            elif body_type == 'Star':
                proto_obj = bodies_pb2.Star()
            else:
                raise Exception('Unsupported EDSM body type')
        if self.file_type == 'powerplay':
            schema = self.schema_mappings.get(self.file_type)
            proto_obj = society_pb2.Powerplay()
        elif self.file_type == 'population':
            schema = self.schema_mappings.get(self.file_type)
            proto_obj = society_pb2.Population()
        elif self.file_type == 'stations':
            schema = self.schema_mappings.get(self.file_type)
            proto_obj = settlement_pb2.Settlement()
        elif self.file_type == 'systems':
            schema = self.schema_mappings.get(self.file_type)
            proto_obj = system_pb2.System()

        self._map_proto_fields(proto_obj, schema, self.json_dict)
        # print(self.json_dict)
        # print(proto_obj)
        # print('\n\n')
        return proto_obj
