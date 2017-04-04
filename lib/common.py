import decimal
import json


def floats_to_decimals(obj):
    if isinstance(obj, list):
        for i in xrange(len(obj)):
            obj[i] = floats_to_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.iterkeys():
            obj[k] = floats_to_decimals(obj[k])
        return obj
    elif isinstance(obj, float):
        if obj % 1 == 0:
            return int(obj)
        else:
            return decimal.Decimal(obj)
    else:
        return obj

def replace_decimals(obj):
    if isinstance(obj, list):
        for i in xrange(len(obj)):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.iterkeys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, decimal.Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


    
def gen_json_resp(d,code='200'):
    return {'statusCode': code,
            'body': json.dumps(d,cls=DecimalEncoder),
            'headers': {
                'Content-Type': 'application/json'
                }
        }
