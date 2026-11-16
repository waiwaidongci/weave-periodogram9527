import json,uuid
from datetime import datetime,timezone,timedelta
from pathlib import Path
from sqlalchemy import select,func,update,or_,and_
from src.config.settings import ARTIFACT_DIR,PERM_SHARD_SIZE,SHARD_LEASE_SECONDS
from src.models import SpectrumRun,PermutationShard,AuditLog
from .analysis import observed_base,finalize_result
from .permutation import shard_layout,shard_checkpoints,shard_null_maxima
from .spectrum import centered_response
from .qr_cache import build_cache
from .validation import validate_points,validate_plan
from .audit import record

def utcnow():return datetime.now(timezone.utc)

def plan_run(db,run_id):
 # 排队时已冻结测点与计划快照；规划只读快照，重复进入幂等
 run=db.execute(select(SpectrumRun).where(SpectrumRun.id==run_id).with_for_update().execution_options(populate_existing=True)).scalar_one_or_none()
 if not run or run.status!='queued':return False
 if db.scalar(select(func.count()).select_from(PermutationShard).where(PermutationShard.run_id==run_id)):
  run.status='running';db.commit();return True
 snapshot=run.source_snapshot;plan=run.plan_snapshot
 staging,centered,cache=observed_base(snapshot['study'],snapshot['points'],plan)
 sizes=shard_layout(plan['permutations'],PERM_SHARD_SIZE);states=shard_checkpoints(plan['seed'],len(centered),sizes)
 start=0
 for index,(size,state) in enumerate(zip(sizes,states)):
  db.add(PermutationShard(run_id=run_id,shard_index=index,start_index=start,perm_count=size,rng_state=state));start+=size
 run.observed_part=staging;run.status='running';run.error=None;db.commit()
 record(db,'worker','run.planned',{'run_id':run_id,'shards':len(sizes),'permutations':plan['permutations'],'shard_size':PERM_SHARD_SIZE})
 return True

def dispatch_pending_shards(db,run_id,force=False):
 now=utcnow();query=select(PermutationShard).where(PermutationShard.run_id==run_id,PermutationShard.status=='pending')
 if not force:query=query.where(or_(PermutationShard.last_dispatch_at==None,PermutationShard.last_dispatch_at<now-timedelta(seconds=60)))
 shards=db.scalars(query.order_by(PermutationShard.shard_index)).all()
 if shards:
  from src.worker import perm_shard_task
  for shard in shards:shard.last_dispatch_at=now;perm_shard_task.delay(shard.id)
  db.commit()
 return len(shards)

def execute_shard(db,shard_id,owner):
 shard=db.get(PermutationShard,shard_id)
 if not shard or shard.status=='done':return 'skipped'
 run=db.get(SpectrumRun,shard.run_id)
 if not run or run.status!='running':return 'aborted'
 token=uuid.uuid4().hex;now=utcnow()
 acquired=db.execute(update(PermutationShard).where(PermutationShard.id==shard_id).where(and_(or_(PermutationShard.status=='pending',and_(PermutationShard.status=='leased',PermutationShard.lease_expires_at<now)),PermutationShard.run_id==run.id)).values(status='leased',lease_owner=owner,lease_token=token,lease_expires_at=now+timedelta(seconds=SHARD_LEASE_SECONDS),attempts=PermutationShard.attempts+1).execution_options(synchronize_session=False)).rowcount
 db.commit()
 if not acquired:return 'skipped'
 snapshot=run.source_snapshot;plan=run.plan_snapshot
 points=validate_points(snapshot['points']);f=validate_plan(plan)
 cache=build_cache([p['position'] for p in points],f);centered,_,rss0=centered_response([p['thickness'] for p in points])
 nulls=shard_null_maxima(cache,centered,rss0,shard.rng_state,shard.perm_count)
 # 围栏令牌：失联分片被接管后，原执行者晚到的提交不匹配令牌，拒收
 submitted=db.execute(update(PermutationShard).where(PermutationShard.id==shard_id,PermutationShard.lease_token==token,PermutationShard.status=='leased').values(status='done',null_maxima=nulls,completed_at=utcnow(),lease_expires_at=None).execution_options(synchronize_session=False)).rowcount
 if not submitted:
  db.rollback();record(db,'worker','shard.lease_lost',{'shard_id':shard_id,'run_id':run.id,'owner':owner});return 'lease_lost'
 db.execute(update(SpectrumRun).where(SpectrumRun.id==run.id).values(done_permutations=SpectrumRun.done_permutations+shard.perm_count).execution_options(synchronize_session=False))
 db.commit()
 finalize_if_ready(db,run.id)
 return 'done'

def finalize_if_ready(db,run_id):
 # 全部分片齐备才发布全谱与结论；行锁保证只发布一次，缺号拒绝发布
 run=db.execute(select(SpectrumRun).where(SpectrumRun.id==run_id).with_for_update().execution_options(populate_existing=True)).scalar_one_or_none()
 if not run or run.status!='running':return False
 total,done=db.execute(select(func.count(),func.count().filter(PermutationShard.status=='done')).where(PermutationShard.run_id==run_id)).one()
 if not total or done<total:return False
 shards=db.scalars(select(PermutationShard).where(PermutationShard.run_id==run_id).order_by(PermutationShard.shard_index).execution_options(populate_existing=True)).all()
 nulls=[value for shard in shards for value in shard.null_maxima];plan=run.plan_snapshot
 if len(nulls)!=plan['permutations']:
  db.rollback();record(db,'worker','run.finalize_blocked',{'run_id':run_id,'nulls':len(nulls),'expected':plan['permutations']});return False
 result=finalize_result(run.observed_part,nulls,plan['seed']);artifact_error=None
 try:
  Path(ARTIFACT_DIR).mkdir(parents=True,exist_ok=True);Path(ARTIFACT_DIR,f'weave-{run_id}.json').write_text(json.dumps(result,ensure_ascii=False,allow_nan=False))
 except OSError as error:artifact_error=str(error)[:200]
 run.result=result;run.peak_frequency=result['best']['frequency'];run.max_power=result['observed_max_power'];run.global_p=result['global_test']['global_p'];run.status='completed';run.error=None
 db.add(AuditLog(actor='worker',action='run.completed',detail={'run_id':run_id,'global_p':run.global_p,'permutations':len(nulls)}))
 if artifact_error:db.add(AuditLog(actor='worker',action='run.artifact_failed',detail={'run_id':run_id,'error':artifact_error}))
 db.commit();return True

def sweep(db):
 # 失联分片租约到期回收重投；排队运行补投规划；齐备运行补发布
 now=utcnow()
 reset=db.execute(update(PermutationShard).where(PermutationShard.status=='leased',PermutationShard.lease_expires_at<now,PermutationShard.run_id.in_(select(SpectrumRun.id).where(SpectrumRun.status=='running'))).values(status='pending',lease_owner=None,lease_token=None,lease_expires_at=None).execution_options(synchronize_session='fetch')).rowcount
 db.commit()
 queued=db.scalars(select(SpectrumRun.id).where(SpectrumRun.status=='queued')).all()
 if queued:
  from src.worker import plan_run_task
  for run_id in queued:plan_run_task.delay(run_id)
 dispatched=0
 for run_id in db.scalars(select(SpectrumRun.id).where(SpectrumRun.status=='running')).all():
  finalize_if_ready(db,run_id);dispatched+=dispatch_pending_shards(db,run_id)
 return {'reset_leases':reset,'queued_dispatched':len(queued),'shards_dispatched':dispatched}

def resume_run(db,run_id,actor):
 run=db.get(SpectrumRun,run_id)
 if not run:return None
 now=utcnow()
 reset=db.execute(update(PermutationShard).where(PermutationShard.run_id==run_id,PermutationShard.status=='leased',PermutationShard.lease_expires_at<now).values(status='pending',lease_owner=None,lease_token=None,lease_expires_at=None).execution_options(synchronize_session='fetch')).rowcount
 db.commit()
 dispatched=0
 if run.status=='queued':
  from src.worker import plan_run_task;plan_run_task.delay(run_id);dispatched='plan'
 elif run.status=='running':
  dispatched=dispatch_pending_shards(db,run_id,force=True);finalize_if_ready(db,run_id)
 record(db,actor,'run.resumed',{'run_id':run_id,'reset_leases':reset,'dispatched':dispatched})
 return {'run_id':run_id,'status':run.status,'reset_leases':reset,'dispatched':dispatched}

def run_progress(db,run_id):
 run=db.get(SpectrumRun,run_id)
 if not run:return None
 total=run.plan_snapshot['permutations'];shards=db.scalars(select(PermutationShard).where(PermutationShard.run_id==run_id).order_by(PermutationShard.shard_index)).all()
 done=sum(1 for s in shards if s.status=='done');leased=sum(1 for s in shards if s.status=='leased')
 return {'run_id':run_id,'status':run.status,'permutations':total,'done_permutations':total if run.status=='completed' else run.done_permutations,'shard_size':shards[0].perm_count if shards else None,'total_shards':len(shards),'done_shards':done,'leased_shards':leased,'pending_shards':len(shards)-done-leased,'shards':[{'index':s.shard_index,'status':s.status,'count':s.perm_count,'attempts':s.attempts,'lease_owner':s.lease_owner,'completed_at':s.completed_at.isoformat() if s.completed_at else None} for s in shards]}
