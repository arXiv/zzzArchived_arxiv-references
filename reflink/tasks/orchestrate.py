from celery.exceptions import TaskError


def process_document(document_id):
    print ("!!", document_id)
    try:
        task_id = None  # TODO: here is where we kick off the processing chain.
    except TaskError as e:
        raise IOError('Could not create processing tasks: %s' % e)
    return task_id
