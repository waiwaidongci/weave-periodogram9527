import socket
from celery import Celery
from celery.signals import worker_ready
from src.config.database import SessionLocal
from src.config.settings import REDIS_URL,SWEEP_INTERVAL_SECONDS
from src.models import SpectrumRun
from src.services.audit import record
app=Celery('weave_periodogram',broker=REDIS_URL,backend=REDIS_URL)
app.conf.update(task_track_started=True,worker_prefetch_multiplier=1,task_acks_late=True,beat_schedule={'shard-sweeper':{'task':'weave.sweeper','schedule':SWEEP_INTERVAL_SECONDS}})
OWNER=f'worker@{socket.gethostname()}'
def fail_run(db,id,error):
 db.rollback();run=db.get(SpectrumRun,id)
 if run and run.status in ('queued','running'):
  run.status='failed';run.error=str(error)[:1000];db.commit();record(db,'worker','run.failed',{'run_id':id,'error':run.error})
@app.task(name='weave.plan_run',soft_time_limit=300,time_limit=330)
def plan_run_task(id):
 from src.services.shards import plan_run,dispatch_pending_shards
 with SessionLocal() as db:
  try:
   if plan_run(db,id):dispatch_pending_shards(db,id,force=True)
  except ValueError as error:fail_run(db,id,error)
@app.task(name='weave.perm_shard',soft_time_limit=240,time_limit=300)
def perm_shard_task(shard_id):
 from src.services.shards import execute_shard
 with SessionLocal() as db:
  try:execute_shard(db,shard_id,OWNER)
  except ValueError as error:
   db.rollback()
   from src.models import PermutationShard
   shard=db.get(PermutationShard,shard_id)
   if shard:fail_run(db,shard.run_id,error)
@app.task(name='weave.sweeper')
def sweeper_task():
 from src.services.shards import sweep
 with SessionLocal() as db:sweep(db)
@worker_ready.connect
def recover_on_start(**kwargs):
 # worker重启即接管：扫描待规划/待执行/待发布的进度
 sweeper_task.delay()
