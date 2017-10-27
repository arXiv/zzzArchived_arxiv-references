"""Provides access to EC2 instance role credentials."""

import requests
import os
from datetime import datetime, timedelta
from references import logging
from .util import get_application_config, get_application_global

logger = logging.getLogger(__name__)

DEF_ENDPT = "http://169.254.169.254/latest/meta-data/iam/security-credentials"


class CredentialsSession(object):
    """Responsible for maintaining current access credentials for this role."""

    fmt = "%Y-%m-%dT%H:%M:%SZ"

    def __init__(self, endpoint, role, config):
        """Set the instance metadata URL."""
        logger.debug('New CredentialsSession %s' % str(id(self)))
        self.url = '%s/%s' % (endpoint, role)
        self.config = config
        self.get_credentials()

    def _datetime(self, datestring: str) -> datetime:
        """
        Convenience method for parsing a datestring to a datetime.

        Parameters
        ----------
        datestring : str

        Returns
        -------
        :class:`.datetime`
        """
        return datetime.strptime(datestring, self.fmt)

    def _refresh_credentials(self) -> None:
        """Retrieve fresh credentials for the service role."""
        response = requests.get(self.url)
        if not response.ok:
            raise IOError('Could not retrieve credentials')
        data = response.json()
        self.expires = self._datetime(data['Expiration'])
        self.config['AWS_ACCESS_KEY_ID'] = data['AccessKeyId']
        self.config['AWS_SECRET_ACCESS_KEY'] = data['SecretAccessKey']
        self.config['AWS_SESSION_TOKEN'] = data['Token']
        self.config['CREDENTIALS_EXPIRE'] = data['Expiration']

    @property
    def access_key(self) -> str:
        """The current access key id."""
        return self.config.get('AWS_ACCESS_KEY_ID')

    @property
    def secret_key(self) -> str:
        """The current secret access key id."""
        return self.config.get('AWS_SECRET_ACCESS_KEY')

    @property
    def session_token(self) -> str:
        """The current session token."""
        return self.config.get('AWS_SESSION_TOKEN')

    def _get_expires(self) -> datetime:
        """The datetime at which the current credentials expire."""
        expires = self.config.get('CREDENTIALS_EXPIRE')
        if isinstance(expires, str):
            return self._datetime(expires)
        return datetime.now()

    def _set_expires(self, expiry: datetime) -> None:
        """Set the current expiry."""
        if not isinstance(expiry, datetime):
            raise ValueError("Expiry must be a datetime object")
        self.config['CREDENTIALS_EXPIRE'] = expiry.strftime(self.fmt)

    expires = property(_get_expires, _set_expires)

    @property
    def expired(self):
        """Indicate whether the current credentials are expired."""
        # This is padded by 15 seconds, just to be safe.
        return self.expires - datetime.now() <= timedelta(seconds=15)

    def get_credentials(self):
        """Retrieve the current credentials for this role."""
        logger.debug('get credentials...')
        if self.expired or self.access_key is None:
            logger.debug('expired, refreshing')
            self._refresh_credentials()
        logger.debug('new expiry: %s' % self.expires.strftime(self.fmt))
        return self.access_key, self.secret_key, self.session_token


def init_app(app) -> None:
    """Configure an application instance."""
    config = get_application_config(app)
    config.setdefault('CREDENTIALS_ROLE', 'arxiv-references')
    config.setdefault('CREDENTIALS_URL', DEF_ENDPT)


def get_session(app: object = None) -> CredentialsSession:
    """Create a new :class:`.CredentialsSession`."""
    config = get_application_config(app)
    role = config.get('CREDENTIALS_ROLE', "arxiv-references")
    endpoint = config.get('CREDENTIALS_URL', DEF_ENDPT)
    return CredentialsSession(endpoint, role, config)


def current_session(app: object = None):
    """Get/create :class:`.CredentialsSession` for this context."""
    g = get_application_global()
    if g:
        if 'credentials' not in g:
            g.credentials = get_session(app)
        g.credentials.get_credentials()
        return g.credentials
    return get_session(app)
