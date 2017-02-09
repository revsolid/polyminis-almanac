import decimal
import json

DB_TYPE_NUMERIC = (0, 'N')
DB_TYPE_NUMERIC_PRIMARY_KEY = (1, 'N')
DB_TYPE_DOC =    (0, 'D')
DB_TYPE_ID =     (0, 'I')
DB_TYPE_JSON = (0, 'J')
DB_TYPE_STRING = (0, 'S')

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
    @staticmethod
    def get_key():
        return { 'PlanetId': DB_TYPE_NUMERIC_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        # 'SpacePosition': { 'x': x, 'y': y }
        # 'Temperature'  : { 'Min': m, 'Max': M }
        # 'Ph'           : { 'Min': m, 'Max': M }
        return { 'SpacePosition': DB_TYPE_JSON,
                 'Temperature'  : DB_TYPE_JSON, 
                 'Ph'           : DB_TYPE_JSON,
                 'PlanetName'   : DB_TYPE_STRING }

class Test:
    @staticmethod
    def get_key():
        return { 'TEST_KEY': DB_TYPE_NUMERIC_PRIMARY_KEY, 'TEST_HASH': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return { 'TEST_FIELD': DB_TYPE_NUMERIC }


