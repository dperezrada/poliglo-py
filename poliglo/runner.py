# -*- coding: utf-8 -*-
import sys
import imp
from os import environ
from os.path import dirname, basename, splitext
from time import time

from poliglo.preparation import prepare_worker, get_worker_workflow_data, get_config, get_connection
from poliglo.inputs import get_job_data
from poliglo.variables import REDIS_KEY_QUEUE
from poliglo.status import get_workflow_instance_key, update_workflow_instance_key, update_done_jobs
from poliglo.outputs import write_finalized_job, write_outputs, write_error_job

def default_main(master_mind_url, meta_worker, workflow_instance_func, *args, **kwargs):
    worker_workflows, connection = prepare_worker(master_mind_url, meta_worker)
    if environ.get('TRY_INPUT'):
        import pprint
        raw_data = open(environ.get('TRY_INPUT')).read()
        workflow_instance_data = get_job_data(raw_data)
        worker_workflow_data = get_worker_workflow_data(
            worker_workflows,
            workflow_instance_data,
            workflow_instance_data['workflow_instance']['worker_id']
        )
        pprint.pprint(
            list(
                workflow_instance_func(
                    worker_workflow_data, workflow_instance_data, *args, **kwargs
                )
            )
        )
        return None
    print ' [*] Waiting for data. To exit press CTRL+C'
    while True:
        default_main_inside_wrapper(
            connection, worker_workflows, meta_worker, workflow_instance_func, *args, **kwargs
        )

def default_main_inside_wrapper(
        connection, worker_workflows, meta_worker, workflow_instance_func, *args, **kwargs
    ):
    queue_message = connection.brpop([REDIS_KEY_QUEUE % meta_worker,])
    default_main_inside(
        connection, worker_workflows, queue_message, workflow_instance_func, *args, **kwargs
    )

def default_main_inside(
        connection, worker_workflows, queue_message, workflow_instance_func, *args, **kwargs
    ):
    process_message_start_time = time()
    if queue_message is not None:
        raw_data = queue_message[1]
        try:
            workflow_instance_data = get_job_data(raw_data)
            start_time = get_workflow_instance_key(
                connection,
                workflow_instance_data['workflow_instance']['workflow'],
                workflow_instance_data['workflow_instance']['id'],
                'start_time'
            )
            if not start_time:
                update_workflow_instance_key(
                    connection,
                    workflow_instance_data['workflow_instance']['workflow'],
                    workflow_instance_data['workflow_instance']['id'],
                    'start_time',
                    process_message_start_time
                )
            last_job_id = workflow_instance_data['jobs_ids'][-1]
            worker_id = workflow_instance_data['workflow_instance']['worker_id']
            worker_workflow_data = get_worker_workflow_data(
                worker_workflows, workflow_instance_data,
                workflow_instance_data['workflow_instance']['worker_id']
            )
            nodata = True
            for worker_output_data in workflow_instance_func(
                    worker_workflow_data, workflow_instance_data, *args, **kwargs
                ):
                nodata = False
                if not worker_output_data:
                    worker_output_data = {}
                if worker_output_data.get('__next_workers'):
                    worker_workflow_data['next_workers'] = worker_output_data.get(
                        '__next_workers', []
                    )
                if len(worker_workflow_data.get('next_workers', [])) == 0:
                    # aqui
                    write_finalized_job(
                        workflow_instance_data, worker_output_data, worker_id, connection
                    )
                    continue
                # aqui
                write_outputs(
                    connection, workflow_instance_data, worker_output_data, worker_workflow_data
                )
            if nodata:
                worker_output_data = {}
                # aqui
                write_finalized_job(
                    workflow_instance_data, worker_output_data, worker_id, connection
                )
            update_done_jobs(
                connection, workflow_instance_data['workflow_instance']['workflow'],
                workflow_instance_data['workflow_instance']['id'], worker_id,
                last_job_id, process_message_start_time
            )
        except Exception, e:
            worker_id = 'unknown'
            try:
                worker_id = workflow_instance_data['workflow_instance']['worker_id']
            except Exception, e:
                pass
            write_error_job(connection, worker_id, raw_data, e)
        # TODO: Manage if worker fails and message is lost

# You can execute this: python -m poliglo.runner file.py
# file.py must have a function called 'process'
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Missing argument: worker python script"
        sys.exit(-1)
    filename = sys.argv[1]
    basepath = dirname(filename)
    module_name = splitext(basename(filename))[0]
    info = imp.find_module(module_name, [basepath])
    module = imp.load_module(module_name, *info)

    # wait_jobs.py has a main function
    main_function = getattr(module, 'main', None)
    if main_function:
        main_function()
    else:
        config = get_config(environ.get('POLIGLO_SERVER_URL'), 'all')
        connection = get_connection(config)
        default_main(
            environ.get('POLIGLO_SERVER_URL'),
            module_name,
            module.process,
            {'connection': connection}
        )
