import datetime


from db_lib.base_dao import BaseDAO


class BaseDO(object):
    dao = None

    def __init__(self, id="", created_at=None, modified_at=""):
        self.id = id
        self.created_at = datetime.datetime.now().strftime("%d/%m/%YT%H:%M:%S")
        self.modified_at = datetime.datetime.now().strftime("%d/%m/%YT%H:%M:%S")


class DAOHelper(type):
    _meta_instance = {}

    def __new__(mcs, name, bases, namespace, **kwargs):
        return super().__new__(mcs, name, bases, namespace)

    def __init__(cls, name, bases, namespace, **kwargs):
        super().__init__(name, bases, namespace)
        cls._meta_instance[cls] = kwargs

    def __call__(cls, *args, **kwargs):
        if cls._meta_instance.get(cls, None):
            cls.dao = BaseDAO(cls.__name__, cls._meta_instance[cls].get("indexes"))
            cls._meta_instance[cls] = None

        return super(DAOHelper, cls).__call__(**kwargs)



