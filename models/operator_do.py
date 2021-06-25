from reserve_entities.base_dataobject import BaseDO, DAOHelper
from reservecli import logger


class OperatorCredentialsDO(BaseDO, metaclass=DAOHelper,
                            indexes={"operator_email": True},
                            authorization={"master"}):
    def __init__(self, operator_email="", password="", **kwargs):
        super().__init__(**kwargs)
        self.operator_email = operator_email
        self.password = password  # FIXME: Use password hash


class OperatorDO(BaseDO, metaclass=DAOHelper,
                 indexes={"email_address": True, "role": False}):

    def __init__(self, first_name="N/A", last_name="N/A", email_address=None, role="", **kwargs):
        super().__init__(**kwargs)
        self.first_name = first_name
        self.last_name = last_name
        self.email_address = email_address
        self.role = role
