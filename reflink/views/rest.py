from flask.views import MethodView


class NotificationConsumerView(MethodView):
    def post(self):
        
