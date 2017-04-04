import boto3
import decimal
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


            #GET - Query Specific Elements

            db_response = dbadapter.db_client.query(
                TableName=tablename,
                ExpressionAttributeValues=get_key_expression_av(cls.get_key(), pk, sk),    ## { ':v1': { 'S': 'No One You Know', }, },
                KeyConditionExpression=get_key_condition_expression(cls.get_key(), pk, sk),
                ProjectionExpression=get_projection_expression(cls.get_schema(), cls.get_key()),
                ConsistentRead=False,
                ReturnConsumedCapacity='NONE'
            )

            print db_response
            if not db_response.get('Count', 0) > 0:
              r = json.dumps({'Error': 'NotFound'})
              return r, 404

            elements = []
            for item in db_response['Items']:
                elem = {}
                for field in cls.get_schema():
                    if not item.has_key(field):
                        continue
                    elem[field] = item[field]
                for kname in cls.get_key():
                    if not item.has_key(kname):
                        continue
                    elem[kname] = item[kname]
                elements.append(get_json(elem, cls.get_key(), cls.get_schema()))

            expect_list = ( (not sk) and len(cls.get_key().keys()) > 1)

            print (not (pk and sk))
            print ( (not sk) and len(cls.get_key().keys()) > 1)

            response = {}
            if not expect_list:
                response = elements[0] 
            else:
                response['Items'] = elements

            return json.dumps(response)

        if request.method == 'PUT':
            ## Update
            table = dbadapter.db_resource.Table(tablename)
            obj = request.json
            key = get_ddb_key(cls.get_key(), pk, sk, flatten=True)
            print key
            table.update_item(
                Key=key,
                UpdateExpression=get_update_expression(obj, cls.get_schema()),
                ExpressionAttributeValues=get_expression_av(obj, cls.get_schema())
            )
            return json.dumps({})

        if request.method == 'DELETE':
            table = dbadapter.db_resource.Table(tablename)
            table.delete_item(
                Key=get_ddb_key(cls.get_key(), pk, sk, flatten=True)
            )
            return json.dumps({})

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


def get_ddb_key(key, pk, sk, flatten=False):
    ddb_key = {}

    for k in key:
        v = key[k]
        if not flatten:
            if v[0] == 1:
                ddb_key[k] = { v[1]: pk }
            else:
                ddb_key[k] = { v[1]: sk }
        else:
            cast_funcs = { 'N': int, 'S': str, 'J': json.dumps }
            if v[0] == 1:
                kv = pk
            else:
                kv = sk
            ddb_key[k] = cast_funcs[v[1]](kv)

    return ddb_key

def get_update_expression(payload, schema):
    expr = 'SET'

    for (i, (k,v)) in enumerate(payload.iteritems()):
        if schema.has_key(k):
            expr += ' %s = :val%i,'%(k, i)

    expr = expr[:-1]
    print 'Update Expression: %s'%expr
    return expr


def get_expression_av(payload, p_schema):
    expression_av = {}

    cast_funcs = { 'N': decimal.Decimal, 'S': str, 'J': json.dumps }
    for (i, (k,v)) in enumerate(payload.iteritems()):
        if p_schema.has_key(k):
            expression_av[':val%i'%i] = cast_funcs[p_schema[k][1]](v) #json.dumps(v)


    print expression_av
    return expression_av

def get_key_expression_av(key, pk, sk):
    expression_av = {}
    for (i, (k,v)) in enumerate(key.iteritems()):
        if v[0] == 1:
            expression_av[':val%i'%i] = { v[1] : pk }
        elif sk != None:
            expression_av[':val%i'%i] = { v[1] : sk }

    return expression_av

def get_key_condition_expression(key, pk, sk):
    expr = ''

    for (i, (k,v)) in enumerate(key.iteritems()):
        if v[0] == 1:
            expr += ' %s = :val%i'%(k, i)
        elif sk != None:
            expr += ' AND %s = :val%i'%(k, i)

    return expr

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
            kts = ('RANGE', 'HASH')
            print k
            print v
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
                'ReadCapacityUnits': 1,
                'WriteCapacityUnits': 1
            })

if __name__ == '__main__':
  dba = DBAdapter()
