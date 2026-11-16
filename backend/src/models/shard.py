from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import ForeignKey,String,JSON,DateTime,UniqueConstraint
from datetime import datetime
from src.config.database import Base
class PermutationShard(Base):
 __tablename__='permutation_shards'
 __table_args__=(UniqueConstraint('run_id','shard_index',name='uq_shard_run_index'),)
 id:Mapped[int]=mapped_column(primary_key=True)
 run_id:Mapped[int]=mapped_column(ForeignKey('spectrum_runs.id'),index=True)
 shard_index:Mapped[int]=mapped_column()
 start_index:Mapped[int]=mapped_column()
 perm_count:Mapped[int]=mapped_column()
 rng_state:Mapped[dict]=mapped_column(JSON)
 status:Mapped[str]=mapped_column(String(16),default='pending')
 lease_owner:Mapped[str|None]=mapped_column(String(120),nullable=True)
 lease_token:Mapped[str|None]=mapped_column(String(64),nullable=True)
 lease_expires_at:Mapped[datetime|None]=mapped_column(DateTime(timezone=True),nullable=True)
 last_dispatch_at:Mapped[datetime|None]=mapped_column(DateTime(timezone=True),nullable=True)
 attempts:Mapped[int]=mapped_column(default=0)
 null_maxima:Mapped[list|None]=mapped_column(JSON,nullable=True)
 completed_at:Mapped[datetime|None]=mapped_column(DateTime(timezone=True),nullable=True)
