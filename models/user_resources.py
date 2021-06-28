from models.base_data_object import BaseDO, DAOHelper
import hashlib

class UserDO(BaseDO, metaclass=DAOHelper,
             indexes={"email_address": True, "role": False}):

    def __init__(self, first_name="N/A", last_name="N/A", email_address=None, role="", **kwargs):
        super().__init__(**kwargs)
        self.first_name = first_name
        self.last_name = last_name
        self.email_address = email_address
        self.role = role


class UserCredentialsDO(BaseDO, metaclass=DAOHelper,
                        indexes={"email_address": True},
                        relations={"email_address" : UserDO},
                        authorization={"master", "customer", "manager"}):
    def __init__(self, email_address="", password="", **kwargs):
        kwargs['managed_by'] = email_address
        super().__init__(**kwargs)
        self.email_address = email_address
        self.password = hashlib.sha224(password.encode('utf-8')).hexdigest()

