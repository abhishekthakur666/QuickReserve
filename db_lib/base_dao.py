import asyncio
import logging
from db_lib import DB_OPERATION_CREATE_ENTITY, DB_OPERATION_ENTITY_SAVE, DB_OPERATION_ENTITY_GET, \
    DB_OPERATION_ENTITY_DEL
from db_store.datastore_workers import DBAccessReq, DBAccessResp


logger = None




class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DBClient(metaclass=Singleton):

    def __init__(self, req_queue, ev_loop):
        self._req_queue = req_queue
        self._loop = ev_loop

    @classmethod
    def get_instance(cls):
        return cls(None, None)

    async def _execute_op(self, req):
        logger.debug("Put req in queue async, waiting for result")

        await self._req_queue.put(req)
        logger.debug("Received results successfully from db server workers")
        return await req.result

    def create_table_async(self, table_name, indexes):
        req = DBAccessReq(table_name, DB_OPERATION_CREATE_ENTITY, indexes, self._loop.create_future())
        logger.debug("Put create table req in queue")
        async_res = asyncio.run_coroutine_threadsafe(self._execute_op(req), self._loop)
        return async_res.result()

    def save_async(self, table_name, content):
        req = DBAccessReq(table_name, DB_OPERATION_ENTITY_SAVE, content, self._loop.create_future())
        logger.debug("Put save entity req in queue")
        async_res = asyncio.run_coroutine_threadsafe(self._execute_op(req), self._loop)
        return async_res.result()

    def get_async(self, table_name, filters):
        req = DBAccessReq(table_name, DB_OPERATION_ENTITY_GET, filters, self._loop.create_future())
        logger.debug("Put get entity req in queue")
        async_res = asyncio.run_coroutine_threadsafe(self._execute_op(req), self._loop)
        return async_res.result()

    def del_async(self, table_name, _id):
        req = DBAccessReq(table_name, DB_OPERATION_ENTITY_DEL, _id, self._loop.create_future())
        logger.debug("Put del entity req in queue")
        async_res = asyncio.run_coroutine_threadsafe(self._execute_op(req), self._loop)
        return async_res.result()


class BaseDAO(object):

    def __init__(self, entity_name, indexes=None):
        self.db = DBClient.get_instance()
        self.name = entity_name
        self.indexes = indexes
        self.entity_initialized = False

    def save(self, obj):
        if not self.entity_initialized:
            resp = self.db.create_table_async(self.name, self.indexes)
            if not isinstance(resp, DBAccessResp):
                return False, None

            if not resp.status:
                return resp.status, resp.result

        resp = self.db.save_async(self.name, obj)
        if not isinstance(resp, DBAccessResp):
            return False, None

        return resp.status, resp.result

    def remove(self, _id):
        if not self.entity_initialized:
            resp = self.db.create_table_async(self.name, self.indexes)
            if not isinstance(resp, DBAccessResp):
                return False, None

            if not resp.status:
                return resp.status, resp.result

        resp = self.db.del_async(self.name, _id)
        if not isinstance(resp, DBAccessResp):
            return False, None

        return resp.status, resp.result

    def get(self, filters):
        if not self.entity_initialized:
            resp = self.db.create_table_async(self.name, self.indexes)
            if not isinstance(resp, DBAccessResp):
                return False, None

            if not resp.status:
                return resp.status, resp.result

        resp = self.db.get_async(self.name, filters)
        if not isinstance(resp, DBAccessResp):
            return False, None

        return resp.status, resp.result
