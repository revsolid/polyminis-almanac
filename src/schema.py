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
    return int(field['N'])

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
        'Materials'      : { 'G': g, 'U': u, ...}
    '''
    @staticmethod
    def get_key():
        return { 'PlanetId': DB_TYPE_NUMERIC_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return { 'SpacePosition': DB_TYPE_JSON,
                 'Temperature'  : DB_TYPE_JSON,
                 'Ph'           : DB_TYPE_JSON,
                 'Materials'    : DB_TYPE_JSON,
                 'PlanetName'   : DB_TYPE_STRING,
                 'Epoch'        : DB_TYPE_NUMERIC }

class User:
    @staticmethod
    def get_key():
        return { 'UserName': DB_TYPE_STRING_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return { 'LastKnownPosition': DB_TYPE_JSON }

# Examples of Inventory Entries:
#
# UserName: 'TestUser', 'InventoryEntry': {'Type': BioMass, 'Value': 100 }
# UserName: 'TestUser', 'InventoryEntry': {'Type': SpeciesInStorage, 'Value': { ... Species object ...}}
# UserName: 'TestUser2', 'InventoryEntry': {'Type': BioMass, 'Value': 1100 }
class InventoryEntry:
    @staticmethod
    def get_urlname():
        return 'InventoryEntries'
    @staticmethod
    def get_key():
        return { 'UserName': DB_TYPE_STRING_PRIMARY_KEY }
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

class SpeciesInPlanet:
    @staticmethod
    def get_key():
        return { 'PlanetId': DB_TYPE_NUMERIC_PRIMARY_KEY, 'Epoch': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return { 'SpeciesName': DB_TYPE_STRING,
                 'Proportion': DB_TYPE_NUMERIC,
                 'Individuals': DB_TYPE_JSON,
                 'GAConfiguration': DB_TYPE_JSON,
                 'TranslationTable': DB_TYPE_JSON,
                 'InstinctWeights': DB_TYPE_JSON
               }
    
