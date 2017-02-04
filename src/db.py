import boto3;
import json;
import schema;

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

    # Verify with dbadapter if a table with tablename exists
    # will throw if it can't find the table (TODO: Should create it)
    if not dbadapter.table_exists(tablename):
        raise Exception('Table %s does not exist'%tablename)

    schema = cls.get_schema()

    def crudable_handler(pk, sk = None):
        if request.method == 'POST': 
            obj = request.json
            dbadapter.db_client.put_item(
                TableName=tablename,
                Item=build_item(obj, cls.get_key(), cls.get_schema()),
            )
            return 'happy face'

        if request.method == 'GET': 
            print 'Getting from TableName'
            db_response = dbadapter.db_client.get_item(
                TableName=tablename,
                Key=get_ddb_key(cls.get_key(), pk, sk),
                ConsistentRead=False,
                ReturnConsumedCapacity='NONE',
                ProjectionExpression=get_projection_expression(schema, cls.get_key()),
            )
    
            if not db_response.has_key('Item'):
                return json.dumps({'Error': 'NotFound'})

            response = {}
            for field in cls.get_schema():
                response[field] = db_response['Item'][field]
            for kname in cls.get_key():
                response[kname] = db_response['Item'][kname]

            return get_json(response, cls.get_key(), cls.get_schema())

        if request.method == 'PUT': 
            ## Update
            #  TODO
            pass
        if request.method == 'DELETE': 
            ## Duh... 
            # TODO
            pass

    pk_url = '/persistence/%s/<pk>'%(urlname.lower())
    sk_url = '/persistence/%s/<pk>/<sk>'%(urlname.lower())
    print '  Created endpoints for: %s at %s and %s'%(cls.__name__, pk_url, sk_url)
    app.add_url_rule(pk_url, endpoint=urlname+'_crud', view_func=crudable_handler, methods=['POST', 'GET', 'PUT', 'DELETE'])
    app.add_url_rule(sk_url, endpoint=urlname+'_crud', view_func=crudable_handler, methods=['POST', 'GET', 'PUT', 'DELETE'])

def build_item(json_obj, key, schema_p):
    item = {}
    schema = dict(schema_p);
    schema.update(key)
    for field in json_obj:
        if not schema.has_key(field):
            continue

        v = json_obj[field]
        f = schema[field]

        item[field] = { f[1]: str(v) }
    print item
    return item

def get_json(from_item, key, db_schema):
    res = {}

    for field in from_item:
        field_cfg = None
        if key.has_key(field):
            field_cfg = key[field]
            
        if db_schema.has_key(field):
            field_cfg = db_schema[field]

        if not field_cfg:
            continue

        if field_cfg[1] == 'N':
            res[field] = schema.numeric_field_extract(from_item[field])
 
    return json.dumps(res)

    
     

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
    print 'ProjExp: %s'%projection_exp
    return projection_exp 


class DBAdapter:
    def __init__(self):
        self.db_client = boto3.client('dynamodb')

    def table_exists(self, tablename):
        return tablename in self.db_client.list_tables()['TableNames']

if __name__ == '__main__':
  dba = DBAdapter()
