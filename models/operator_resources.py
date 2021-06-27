from models.base_dataobject import BaseDO, DAOHelper


class OperatorDO(BaseDO, metaclass=DAOHelper,
                 indexes={"email_address": True, "role": False}):

    def __init__(self, first_name="N/A", last_name="N/A", email_address=None, role="", **kwargs):
        super().__init__(**kwargs)
        self.first_name = first_name
        self.last_name = last_name
        self.email_address = email_address
        self.role = role


class OperatorCredentialsDO(BaseDO, metaclass=DAOHelper,
                            indexes={"email_address": True},
                            relations={"email_address" : OperatorDO},
                            authorization={"master"}):
    def __init__(self, email_address="", password="", **kwargs):
        super().__init__(**kwargs)
        self.email_address = email_address
        self.password = password  # FIXME: Use password hash
