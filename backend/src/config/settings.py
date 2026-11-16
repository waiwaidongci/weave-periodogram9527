import os
DATABASE_URL=os.getenv('DATABASE_URL','postgresql+psycopg://weave:weave_demo@db:5432/weave')
REDIS_URL=os.getenv('REDIS_URL','redis://redis:6379/0')
JWT_SECRET=os.environ['JWT_SECRET']
SEED_DEMO=os.getenv('SEED_DEMO','true').lower()=='true'
ARTIFACT_DIR=os.getenv('ARTIFACT_DIR','/artifacts')
RATE_LIMIT_WRITES=int(os.getenv('RATE_LIMIT_WRITES','60'))
PERM_SHARD_SIZE=int(os.getenv('PERM_SHARD_SIZE','1000'))
SHARD_LEASE_SECONDS=int(os.getenv('SHARD_LEASE_SECONDS','300'))
SWEEP_INTERVAL_SECONDS=float(os.getenv('SWEEP_INTERVAL_SECONDS','30'))
