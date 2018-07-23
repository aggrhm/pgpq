CREATE TABLE queues (
  id            serial PRIMARY KEY,
  name          varchar(100) NOT NULL,
  capacity      integer NOT NULL,
  jobs_count    integer DEFAULT 0,
  is_locked     boolean DEFAULT FALSE,
  min_priority  bigint,
  created_at    timestamp NOT NULL,
  updated_at    timestamp NOT NULL
);

CREATE UNLOGGED TABLE jobs (
  id            bigserial PRIMARY KEY,
  queue_name    varchar(100) NOT NULL,
  quid          varchar(200) NOT NULL,
  priority      bigint NOT NULL,
  data          jsonb,
  state         integer NOT NULL,
  state_changed_at  timestamp,
  created_at    timestamp NOT NULL
);

CREATE INDEX jobs_priority_idx ON jobs (queue_name, priority DESC NULLS LAST);
CREATE UNIQUE INDEX jobs_quid_idx ON jobs (queue_name, quid);

CREATE UNIQUE INDEX queues_name_idx ON queues (name, is_locked, min_priority);
