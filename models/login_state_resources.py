from models.base_dataobject import BaseDO, DAOHelper



class LoginStateDO(BaseDO, metaclass=DAOHelper,
                   indexes={"email_address": False}):
    def __init__(self, email_address="", password="", **kwargs):
        super().__init__(**kwargs)
        self.email_address = email_address
        self.password = password[::-1]
