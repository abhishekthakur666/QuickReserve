import datetime


from db_lib.base_dao import BaseDAO


class BaseDO(object):
    dao = None
    authorization = set()

    def __init__(self, id="", created_at="", modified_at="", created_by="", updated_by="", managed_by=""):
        self.id = id
        self.created_at = datetime.datetime.now().strftime("%d/%m/%YT%H:%M:%S") if not created_at else created_at
        self.modified_at = datetime.datetime.now().strftime("%d/%m/%YT%H:%M:%S") if not modified_at else modified_at
        self.created_by = created_by
        self.updated_by = updated_by
        self.managed_by = managed_by or self.updated_by

    @classmethod
    def verify_authorization(cls, role):
        if not cls.authorization:
            return True
        return role in cls.authorization

    def validate(self, obj=None):
        return True, None

class DAOHelper(type):
    _meta_instance = {}

    def __new__(mcs, name, bases, namespace, **kwargs):
        return super().__new__(mcs, name, bases, namespace)

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace)
        cls._meta_instance[cls] = kwargs

    def __call__(cls, *args, **kwargs):
        if cls._meta_instance.get(cls, None):
            cls.dao = BaseDAO(cls.__name__, cls._meta_instance[cls].get("indexes", {}))
            cls.authorization = cls._meta_instance[cls].get("authorization")
            cls.dependent_by = {}
            cls.relations = cls._meta_instance[cls].get("relations", {})
            for k, v in cls.relations.items():
                v.dependent_by[cls] = k
            cls._meta_instance[cls] = None


        return super(DAOHelper, cls).__call__(**kwargs)
