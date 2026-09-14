"""Exercise a million-row queue locally. Never contacts DBD or uses the real DB."""
import argparse
import asyncio
import json
import statistics
import time

from dbd.contracts.identity import CompanyIdentity
from dbd.core.settings import settings
from dbd.storage.mysql.async_engine import create_engine
from dbd.storage.mysql.repositories.worker_repository import WorkerRepository


async def run(count, sample):
    if not settings.db_name.startswith('dbd_test_'):
        raise SystemExit('Benchmark requires a disposable dbd_test_* database')
    engine = create_engine()
    repo = WorkerRepository(engine)
    started = time.perf_counter()
    try:
        run_id = await repo.create_run('synthetic-queue-benchmark',['profile'])
        for offset in range(0, count, 1000):
            ids = [CompanyIdentity(jp_no=str(9000000000000+i),jp_type_code='7')
                   for i in range(offset,min(offset+1000,count))]
            await repo.import_batch(run_id,ids,('profile',))
            if (offset+1000) % 100000 == 0:
                print(f'enqueued {offset+1000:,} / {count:,}', flush=True)
        await repo.ready(run_id)
        enqueue_seconds = time.perf_counter()-started
        timings=[]
        claimed=set()
        lock=asyncio.Lock()
        async def consumer(worker):
            while True:
                async with lock:
                    if len(claimed)>=sample:
                        return
                    # Reservation is a counter, independent of actual task IDs.
                    claimed.add(('slot',len(claimed)))
                begin=time.perf_counter()
                task=await repo.claim(run_id,worker)
                if task is None:
                    raise RuntimeError('Unexpected empty queue')
                await repo.finish(task,'done')
                timings.append(time.perf_counter()-begin)
        begin=time.perf_counter()
        await asyncio.gather(*(consumer(f'benchmark-{i}') for i in range(16)))
        elapsed=time.perf_counter()-begin
        result={'companies':count,'tasks':count,'enqueue_seconds':round(enqueue_seconds,3),
                'sample_tasks':len(timings),'sample_seconds':round(elapsed,3),
                'claim_and_finish_per_second':round(len(timings)/elapsed,2),
                'task_latency_p50_ms':round(statistics.median(timings)*1000,2),
                'task_latency_p95_ms':round(sorted(timings)[int(len(timings)*.95)-1]*1000,2),
                'workers':16,'database':settings.db_name,'api_requests':0}
        print(json.dumps(result,indent=2),flush=True)
    finally:
        await engine.dispose()


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('--companies',type=int,default=1000000)
    parser.add_argument('--sample',type=int,default=1000)
    args=parser.parse_args()
    if not 1<=args.sample<=args.companies<=10000000:
        parser.error('Require 1 <= sample <= companies <= 10000000')
    asyncio.run(run(args.companies,args.sample))
