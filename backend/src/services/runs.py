from sqlalchemy import select
from fastapi import HTTPException
from src.models import SpectrumRun,FrequencyPlan
from src.utils.serialize import row,run_meta
from .source import source_data
from .validation import validate_points,validate_plan
from .audit import record

def create_run(db,data,actor):
 study,points=source_data(db,data.study_id);plan=db.get(FrequencyPlan,data.plan_id)
 if not plan or plan.study_id!=study['id']:raise HTTPException(422,'频率计划不属于该测线')
 snapshot=row(plan)
 try:validate_points(points);validate_plan(snapshot)
 except ValueError as error:raise HTTPException(422,str(error))
 if db.scalar(select(SpectrumRun).where(SpectrumRun.name==data.name)):raise HTTPException(409,'运行名已存在')
 run=SpectrumRun(study_id=study['id'],plan_id=plan.id,name=data.name,source_snapshot={'study':study,'points':points},plan_snapshot=snapshot,created_by=actor,done_permutations=0);db.add(run);db.commit()
 try:
  from src.worker import plan_run_task
  plan_run_task.delay(run.id)
 except Exception:
  # 队列暂时不可用不判失败：运行已安全入队，sweeper与手动恢复都会补投规划
  record(db,'system','run.dispatch_deferred',{'run_id':run.id})
 record(db,actor,'run.queued',{'run_id':run.id,'permutations':snapshot['permutations'],'seed':snapshot['seed']});return run_meta(run)
