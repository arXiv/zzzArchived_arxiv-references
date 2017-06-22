from reflink.tasks import orchestrate


class NotificationController(object):
    def handle_notification(self, notification):
        try:
            task_id = orchestrate.process_document(notification['document_id'])
        except IOError:
            return 500, {'error': 'Failed to begin processing document'}
        return 202, {'task_id': task_id}
