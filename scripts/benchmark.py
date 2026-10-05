#!/usr/bin/env python3
"""Measures full HTTP -> Kafka -> SQL completion, including duplicate HTTP retries.
Run against an otherwise idle stack. Results are written only after assertions pass.
"""
import argparse, concurrent.futures, datetime, json, pathlib, platform, time, uuid
from smoke import request, until
p=argparse.ArgumentParser();p.add_argument('--orders',type=int,default=1000);p.add_argument('--workers',type=int,default=20);a=p.parse_args()
if a.orders<1 or a.workers<1:p.error('orders and workers must be positive')
before=until(lambda:request('/api/metrics'))['summary']; stock_before={s['sku']:s['available'] for s in request('/api/stock')}
if stock_before['HEADPHONES']<a.orders:raise SystemExit('Not enough HEADPHONES stock for this benchmark')
run=str(uuid.uuid4())
def send(i):
    key=f'{run}-{i}';payload={'sku':'HEADPHONES','quantity':1}
    first=request('/api/orders',payload,key)
    second=request('/api/orders',payload,key)
    assert first['id']==second['id']
    return first['id']
start=time.monotonic()
with concurrent.futures.ThreadPoolExecutor(max_workers=a.workers) as pool: ids=list(pool.map(send,range(a.orders)))
submitted=time.monotonic()
assert len(set(ids))==a.orders
# Every successful reservation adds one unique notification and one projected status.
def complete():
    m=request('/api/metrics')['summary']
    return m if m['total_orders']>=before['total_orders']+a.orders and m['reserved']>=before['reserved']+a.orders else None
final=until(complete,180);elapsed=time.monotonic()-start
stock_after={s['sku']:s['available'] for s in request('/api/stock')}
assert final['total_orders']-before['total_orders']==a.orders
assert final['reserved']-before['reserved']==a.orders
assert final['rejected']==before['rejected']
assert stock_before['HEADPHONES']-stock_after['HEADPHONES']==a.orders
result={'measured_at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'host':platform.platform(),'unique_orders':a.orders,'http_requests':a.orders*2,'workers':a.workers,'submission_seconds':round(submitted-start,3),'end_to_end_seconds':round(elapsed,3),'completed_orders_per_second':round(a.orders/elapsed,2),'duplicate_order_effects':0,'stock_deductions':a.orders,'scope':'HTTP ingress plus Kafka outboxes, inventory reservation and SQL analytics; HTTP retry for each order; not a raw Kafka events/sec benchmark'}
pathlib.Path('reports').mkdir(exist_ok=True);pathlib.Path('reports/benchmark.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
