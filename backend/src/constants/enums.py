from enum import StrEnum
class Role(StrEnum):
 ADMIN='admin'
 ANALYST='analyst'
 VIEWER='viewer'
class RunStatus(StrEnum):
 QUEUED='queued'
 RUNNING='running'
 COMPLETED='completed'
 FAILED='failed'
class ShardStatus(StrEnum):
 PENDING='pending'
 LEASED='leased'
 DONE='done'
class NoteStatus(StrEnum):
 DRAFT='draft'
 APPROVED='approved'
 REJECTED='rejected'
