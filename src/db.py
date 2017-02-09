import boto3
import json
import schema
import time

from flask import request

crud_handlers = {}

#####
# The idea is to have CRUDable be a framework-like object that CRUDs
# it registers its endpoints and does some basic validation
def create_crud_endpoints(cls, app, dbadapter):
    if hasattr(cls, 'get_urlname'):
        urlname = cls.get_urlname()
    else:
        urlname = '%ss'%cls.__name__
    print 'Creating Endpoints for: %s at %s'%(cls.__name__, urlname)

    tablename = None
    if hasattr(cls, 'get_tablename'):
        tablename = cls.get_tablename()
    else:
        tablename = urlname

    schema = cls.get_schema()
    # Verify with dbadapter if a table with tablename exists create it if not
    if not dbadapter.table_exists(tablename):
        table = dbadapter.create_table(tablename, cls.get_schema(), cls.get_key())
        print 'Creating table %s...'%tablename
        table.meta.client.get_waiter('table_exists').wait(TableName=tablename)
        print 'Done...'

    def crudable_handler(pk = None, sk = None):
        if request.method == 'POST':
            obj = request.json
            dbadapter.db_client.put_item(
                TableName=tablename,
                Item=build_item(obj, cls.get_key(), cls.get_schema()),
            )
            return '{}'

        if request.method == 'GET':

            if pk == None and sk == None:
                #GET - All elements (Scan DDB)
                table = dbadapter.db_resource.Table(tablename)

                if not table:
                    r = json.dumps({'Error': 'NotFound'})
                    return r, 404

                db_response = table.scan(
                     ProjectionExpression=get_projection_expression(cls.get_schema(), cls.get_key()),
                )

                if not db_response.has_key('Items'):
                    r = json.dumps({'Error': 'NotFound'})
                    return r, 404

                items = [] 
                for item in db_response.get('Items'):
                  items.append(get_json(item, schema, cls.get_key()))

                return json.dumps({'Items': items})


            #GET - Single Element
            db_response = dbadapter.db_client.get_item(
                TableName=tablename,
                Key=get_ddb_key(cls.get_key(), pk, sk),
                ConsistentRead=False,
                ReturnConsumedCapacity='NONE',
                ProjectionExpression=get_projection_expression(cls.get_schema(), cls.get_key()),
            )

            if not db_response.has_key('Item'):
              r = json.dumps({'Error': 'NotFound'})
              return r, 404

            response = {}
            for field in cls.get_schema():
                if not db_response['Item'].has_key(field):
                    continue
                response[field] = db_response['Item'][field]
            for kname in cls.get_key():
                if not db_response['Item'].has_key(kname):
                    continue
                response[kname] = db_response['Item'][kname]

            return json.dumps(get_json(response, cls.get_key(), cls.get_schema()))

        if request.method == 'PUT':
            ## Update
            #  TODO
            pass
        if request.method == 'DELETE':
            ## Duh...
            # TODO
            pass

    all_items_url = '/persistence/%s/'%(urlname.lower())
    app.add_url_rule(all_items_url, endpoint=urlname+'_crud', view_func=crudable_handler, methods=['POST', 'GET', 'PUT', 'DELETE'])
    print '  Created endpoint for: %s at %s'%(cls.__name__, all_items_url)

    pk_url = '/persistence/%s/<pk>'%(urlname.lower())
    sk_url = '/persistence/%s/<pk>/<sk>'%(urlname.lower())
    app.add_url_rule(pk_url, endpoint=urlname+'_crud', view_func=crudable_handler, methods=['POST', 'GET', 'PUT', 'DELETE'])
    app.add_url_rule(sk_url, endpoint=urlname+'_crud', view_func=crudable_handler, methods=['POST', 'GET', 'PUT', 'DELETE'])
    print '  Created endpoints for: %s at %s and %s'%(cls.__name__, pk_url, sk_url)

def build_item(json_obj, key, schema_p):
    print json_obj
    item = {}
    schema = dict(schema_p);
    schema.update(key)
    for field in json_obj:
        if not schema.has_key(field):
            continue

        v = json_obj[field]
        f = schema[field][1]
        
        if f == 'J':
          f = 'S'
          v = json.dumps(v)

        item[field] = { f: str(v) }

    return item

def get_json(from_item, key, db_schema):
    res = {}

    print from_item
    extraction_funcs = { 'N': schema.numeric_field_extract, 'J': schema.json_field_extract, 'S': schema.string_field_extract }

    for field in from_item:
        field_cfg = None
        if key.has_key(field):
            field_cfg = key[field]

        if db_schema.has_key(field):
            field_cfg = db_schema[field]

        if not field_cfg:
            continue

        print field_cfg
        res[field] = extraction_funcs[field_cfg[1]](from_item[field])

    return res


def get_ddb_key(key, pk, sk):
    ddb_key = {}

    for k in key:
        v = key[k]
        if v[0] == 1:
            ddb_key[k] = { v[1]: pk }
        else:
            ddb_key[k] = { v[1]: sk }

    return ddb_key

def get_projection_expression(schema, pk):
    keys = list(schema.keys())
    keys.extend(list(pk.keys()))
    projection_exp = (('%s,'*len(keys))[:-1]%tuple(keys))
    return projection_exp



class DBAdapter:
    def __init__(self):
        self.db_client = boto3.client('dynamodb')
        self.db_resource = boto3.resource('dynamodb')

    def table_exists(self, tablename):
        return tablename in self.db_client.list_tables()['TableNames']

    def create_table(self, tablename, schema, key):

        key_schema = []
        attr_defs  = []

        for (k,v) in key.iteritems():
            kts = ('HASH', 'RANGE')
            key_schema.append({'AttributeName': k, 'KeyType': kts[v[0]]})
            attr_defs.append({'AttributeName': k, 'AttributeType': v[1]})

        if key_schema[0]['KeyType'] != 'HASH':
           key_schema.reverse()

        print ''' Creating Table %s with: Key: %s Attributes: %s ''' % (
              tablename, key_schema, attr_defs)

        return self.db_resource.create_table(
            TableName=tablename,
            KeySchema=key_schema,
            AttributeDefinitions=attr_defs,
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            })

if __name__ == '__main__':
  dba = DBAdapter()
