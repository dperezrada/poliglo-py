# -*- coding: utf-8 -*-

REDIS_KEY_QUEUE = 'queue:%s'
REDIS_KEY_QUEUE_FINALIZED = 'queue:finalized'
REDIS_KEY_INSTANCES = 'workflows:%s:workflow_instances'
REDIS_KEY_ONE_INSTANCE = "workflows:%s:workflow_instances:%s"
REDIS_KEY_INSTANCE_WORKER_TIMING = "workflows:%s:workflow_instances:%s:workers:%s:timing"
REDIS_KEY_INSTANCE_WORKER_FINALIZED_JOBS = "workflows:%s:workflow_instances:%s:workers:%s:finalized"
REDIS_KEY_INSTANCE_WORKER_JOBS = "workflows:%s:workflow_instances:%s:workers:%s:jobs_ids:%s"
REDIS_KEY_INSTANCE_WORKER_ERRORS = "workflows:%s:workflow_instances:%s:workers:%s:errors"
REDIS_KEY_INSTANCE_WORKER_DISCARDED = "workflows:%s:workflow_instances:%s:workers:%s:discarded"

POLIGLO_SERVER_URL_WORKER_CONFIG = "%s/meta_workers/%s/config"
POLIGLO_SERVER_URL_WORKER_WORKFLOWS = "%s/meta_workers/%s/workflows"
