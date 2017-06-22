from flask.views import MethodView
from flask import request, url_for
from reflink.controllers import NotificationController


class NotificationConsumerView(MethodView):
    def __init__(self, *args, **kwargs):
        super(self, NotificationConsumerView).__init__(*args, **kwargs)
        self.controller = NotificationController()

    def post(self):
        data = request.form
