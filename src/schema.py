import decimal
import json

DB_TYPE_NUMERIC = (0, 'N')
DB_TYPE_NUMERIC_PRIMARY_KEY = (1, 'N')
DB_TYPE_DOC =    (0, 'D')
DB_TYPE_ID =     (0, 'I')
DB_TYPE_JSON = (0, 'J')

def numeric_field_extract(field):
    if isinstance(field, decimal.Decimal):
        if field % 1 > 0:
            return float(field)
        else:
            return int(field)
    return int(field['N'])

def json_field_extract(field):
    print field

    if isinstance(field, basestring):
        return json.loads(field)

    return json.loads(field['S'])

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
        return { 'SpacePosition': DB_TYPE_JSON }

class Test:
    @staticmethod
    def get_key():
        return { 'TEST_KEY': DB_TYPE_NUMERIC_PRIMARY_KEY, 'TEST_HASH': DB_TYPE_NUMERIC }
    @staticmethod
    def get_schema():
        return { 'TEST_FIELD': DB_TYPE_NUMERIC }


