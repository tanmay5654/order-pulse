#!/usr/bin/env python3
"""HTTP end-to-end check; waits for the stack, then verifies one deduplicated order."""
import json, os, time, uuid, urllib.request, urllib.error
BASE=os.environ.get('BASE_URL','http://localhost:8090')
def request(path, payload=None, key=None):
    headers={'Content-Type':'application/json'}
    if key: headers['Idempotency-Key']=key
    req=urllib.request.Request(BASE+path,data=json.dumps(payload).encode() if payload is not None else None,headers=headers)
    with urllib.request.urlopen(req,timeout=10) as r: return json.load(r)
def until(fn,seconds=240):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        try:
            value=fn()
            if value: return value
        except (OSError,ValueError): pass
        time.sleep(1)
    raise AssertionError('Timed out waiting for end-to-end state')
if __name__=='__main__':
    before=until(lambda:request('/api/metrics'))['summary']['total_orders']
    stock=next(s['available'] for s in request('/api/stock') if s['sku']=='HEADPHONES')
    key=str(uuid.uuid4()); payload={'sku':'HEADPHONES','quantity':1}
    first=request('/api/orders',payload,key);second=request('/api/orders',payload,key)
    assert first['id']==second['id'],'HTTP retry created a duplicate order'
    def done():
        m=request('/api/metrics')
        return m if any(r['order_id']==first['id'] and r['status']=='StockReserved' for r in m['recent']) else None
    metrics=until(done,60)
    assert metrics['summary']['total_orders']==before+1,'Analytics counted duplicate order'
    assert next(s['available'] for s in request('/api/stock') if s['sku']=='HEADPHONES')==stock-1,'Stock deducted incorrectly'
    print('PASS: HTTP retry -> one order, one stock deduction, one SQL aggregate, final reservation visible')
