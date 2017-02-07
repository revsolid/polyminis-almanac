import decimal

NUMERIC = (0, 'N')
NUMERIC_PRIMARY_KEY = (1, 'N')
DB_TYPE_DOC =    (0, 'D')
DB_TYPE_ID =     (0, 'I')

def numeric_field_extract(field):
    if isinstance(field, decimal.Decimal):
        if field % 1 > 0:
            return float(field)
        else:
            return int(field)
    return int(field['N'])

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
        return { 'PlanetId': NUMERIC_PRIMARY_KEY,
                 'EpochNum': NUMERIC }
    @staticmethod
    def get_schema():
        return {}

class SpeciesSeed:
    @staticmethod
    def get_key():
        return { 'CreatorId': NUMERIC_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return {}

class Planet:
    @staticmethod
    def get_key():
        return { 'PlanetId': NUMERIC_PRIMARY_KEY }
    @staticmethod
    def get_schema():
        return {}

class Test:
    @staticmethod
    def get_key():
        return { 'TEST_KEY': NUMERIC_PRIMARY_KEY, 'TEST_HASH': NUMERIC }
    @staticmethod
    def get_schema():
        return { 'TEST_FIELD': NUMERIC }


