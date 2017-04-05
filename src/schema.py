import decimal
import json

DB_TYPE_NUMERIC = (0, 'N')
DB_TYPE_NUMERIC_PRIMARY_KEY = (1, 'N')
DB_TYPE_DOC =    (0, 'D')
DB_TYPE_ID =     (0, 'I')
DB_TYPE_JSON = (0, 'J')
DB_TYPE_STRING = (0, 'S')
DB_TYPE_STRING_PRIMARY_KEY = (1, 'S')

def numeric_field_extract(field):
    if isinstance(field, decimal.Decimal):
        if field % 1 > 0:
            return float(field)
        else:
            return int(field)
    try:
        return int(field['N'])
    except ValueError:
        return float(field['N'])

def json_field_extract(field):
    s_field = string_field_extract(field)
    return json.loads(s_field)

def string_field_extract(field):
    if isinstance(field, basestring):
        return str(field)

    return str(field['S'])

def id_field_extract(field):
    pass

def doc_field_extract(field):
    pass


####
# These represent our CRUDable objects, each one of them is a DynamoDB table and there's a 1:1 matching (or really close) between
# entries in the Key / Schema Dictionaries and the actual tables

class Epoch:
    '''
    Epoch - 
    '''
    @staticmethod
    def get_key():
        return { 'PlanetId': DB_TYPE_NUMERIC_PRIMARY_KEY,
                 'EpochNum': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return {}

class SpeciesSeed:
    @staticmethod
    def get_key():
        return { 'CreatorId': DB_TYPE_NUMERIC_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return {}

class Planet:
    '''
    Planet -  
        'PlanetName'     : 'PlanetName',
        'SpacePosition'  : { 'x': x, 'y': y },
        'Temperature'    : { 'Min': m, 'Max': M },
        'Ph'             : { 'Min': m, 'Max': M },
        'Density'        : [0..1],
        'Epoch'          : N > 0,
        'Environment'    : { ... polyminis_core::Environment ... }
        'Materials'      : { 'G': g, 'U': u, ...}
    '''
    @staticmethod
    def get_key():
        return { 'PlanetId': DB_TYPE_NUMERIC_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return { 
                 'PlanetName'   : DB_TYPE_STRING,
                 'SpacePosition': DB_TYPE_JSON,
                 'Temperature'  : DB_TYPE_JSON,
                 'Ph'           : DB_TYPE_JSON,
                 'Density'      : DB_TYPE_NUMERIC,
                 'Epoch'        : DB_TYPE_NUMERIC,
                 'Environment'  : DB_TYPE_JSON }

class User:
    @staticmethod
    def get_key():
        return { 'UserName': DB_TYPE_STRING_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return { 'LastKnownPosition': DB_TYPE_JSON, 'Biomass': DB_TYPE_NUMERIC, 'InventorySlots': DB_TYPE_NUMERIC }

# Examples of Inventory Entries:
#
# UserName: 'TestUser', Slot: 0,  'InventoryEntry': {'Type': SpeciesSeed, 'Value': { ... Species object ...}}
# UserName: 'TestUser', Slot: 1, 'InventoryEntry': {'Type': Research, 'Value': { ... Research object ...}}
# UserName: 'TestUser2', Slot: 0, 'InventoryEntry': {'Type': SpeciesSeed, 'Value': { ... Species object ...}}
class InventoryEntry:
    @staticmethod
    def get_urlname():
        return 'InventoryEntries'
    @staticmethod
    def get_key():
        return { 'UserName': DB_TYPE_STRING_PRIMARY_KEY, 'Slot': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return { 'InventoryEntry': DB_TYPE_JSON }

# Species Owned by the player deployed on a Planet
class OwnedSpecies:
    @staticmethod
    def get_urlname():
        return 'OwnedSpecies'
    @staticmethod
    def get_key():
        return { 'UserName': DB_TYPE_STRING_PRIMARY_KEY, 'PlanetId': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return { 'SpeciesName': DB_TYPE_STRING }

# To track the last time a player visited a Planet, an absence of a record means, obviously that planet has
# not been visited by the player
class PlanetVisit:
    @staticmethod
    def get_key():
        return { 'UserName': DB_TYPE_STRING_PRIMARY_KEY, 'PlanetId': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return { 'VisitData': DB_TYPE_JSON }


class SpeciesSummary:
    @staticmethod
    def get_urlname():
        return 'SpeciesSummaries'
    @staticmethod
    def get_tablename():
        return 'SpeciesInPlanet'
    @staticmethod
    def get_key():
        return { 'PlanetEpoch': DB_TYPE_STRING_PRIMARY_KEY, 'SpeciesName': DB_TYPE_STRING}
    @staticmethod
    def get_schema():
        schema = SpeciesInPlanet.get_schema()
        del schema['Individuals']
        return schema

class SpeciesInPlanet:
    @staticmethod
    def get_urlname():
        return 'SpeciesInPlanet'
    @staticmethod
    def get_tablename():
        return 'SpeciesInPlanet'
    @staticmethod
    def get_key():
        return { 'PlanetEpoch': DB_TYPE_STRING_PRIMARY_KEY, 'SpeciesName': DB_TYPE_STRING}
    @staticmethod
    def get_schema():
        return { 'Percentage': DB_TYPE_NUMERIC,
                 'Individuals': DB_TYPE_JSON,
                 'CreatorName': DB_TYPE_STRING,
                 'GAConfiguration':  DB_TYPE_JSON,
                 'Splices':          DB_TYPE_JSON,
                 'TranslationTable': DB_TYPE_JSON, 
                 'InstinctWeights': DB_TYPE_JSON,
                 'InstinctTuning': DB_TYPE_JSON
               }

class GameRules:
    @staticmethod
    def get_urlname():
        return 'GameRules'
    @staticmethod
    def get_tablename():
        return 'GameRules'
    @staticmethod
    def get_key():
        return { 'VERSION': DB_TYPE_STRING_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return { 'WarpCostCurveKeyframes': DB_TYPE_JSON,
                 'PercentageToBiomassCurveKeyframes': DB_TYPE_JSON,
                 'BiomassToPercentageCurveKeyframes': DB_TYPE_JSON,
                 'BaseWarpCost': DB_TYPE_NUMERIC,
                 'WarpCostMultiplier': DB_TYPE_NUMERIC,
                 'MaxWarpDistance': DB_TYPE_NUMERIC,
                 'SpliceData': DB_TYPE_JSON,
                 'TraitData': DB_TYPE_JSON,
                 'DefaultTraits': DB_TYPE_JSON
                }

class EpochCounter:
    @staticmethod
    def get_urlname():
        return 'EpochCounter'
    @staticmethod
    def get_tablename():
        return 'EpochCounter'
    @staticmethod
    def get_key():
        return { 'VERSION': DB_TYPE_STRING_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return { 'Epoch': DB_TYPE_NUMERIC }
