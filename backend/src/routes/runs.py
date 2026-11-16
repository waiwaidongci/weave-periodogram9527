from fastapi import APIRouter,Depends,HTTPException
from sqlalchemy import select
from sqlalchemy.orm import Session
from src.config.database import get_db
from src.controllers.dependencies import current_user,writers
from src.models import SpectrumRun
from src.schemas.run import RunInput
from src.services.runs import create_run
from src.services.shards import run_progress,resume_run
from src.utils.serialize import run_meta,run_detail
router=APIRouter()
@router.get('/runs')
def runs(db:Session=Depends(get_db),user=Depends(current_user)):return [run_meta(r) for r in db.scalars(select(SpectrumRun).order_by(SpectrumRun.id.desc()))]
@router.get('/runs/{id}')
def one(id:int,db:Session=Depends(get_db),user=Depends(current_user)):
 run=db.get(SpectrumRun,id)
 if not run:raise HTTPException(404,'运行不存在')
 return run_detail(run)
@router.get('/runs/{id}/progress')
def progress(id:int,db:Session=Depends(get_db),user=Depends(current_user)):
 data=run_progress(db,id)
 if not data:raise HTTPException(404,'运行不存在')
 return data
@router.post('/runs')
def create(data:RunInput,db:Session=Depends(get_db),user=Depends(writers)):return create_run(db,data,user.email)
@router.post('/runs/{id}/resume')
def resume(id:int,db:Session=Depends(get_db),user=Depends(writers)):
 run=db.get(SpectrumRun,id)
 if not run:raise HTTPException(404,'运行不存在')
 if run.status=='completed':raise HTTPException(409,'运行已完成，全部置换已发布')
 if run.status=='failed':raise HTTPException(409,f'运行已失败：{run.error or "未知错误"}，请修正后重建')
 return resume_run(db,id,user.email)
