from datetime import datetime,date
from sqlalchemy import inspect
def row(obj):
 return {c.key:(getattr(obj,c.key).isoformat() if isinstance(getattr(obj,c.key),(datetime,date)) else getattr(obj,c.key)) for c in inspect(obj).mapper.column_attrs if c.key!='password_hash'}
def run_meta(run):
 # 列表与创建响应：不携带result/observed_part等大字段
 data=row(run);data.pop('result',None);data.pop('observed_part',None);return data
def run_detail(run):
 data=row(run);data.pop('observed_part',None);return data
