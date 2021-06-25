import asyncio
import json
from asyncio import Future

from db_store import MAX_TASK_QUEUE_SIZE, TABLE_NOT_FOUND, DEFAULT_UUID_LEN, \
    ENTITY_NOT_FOUND, DUPLICATE_ENTITY_FOUND, DB_OPERATION_CREATE_ENTITY, DB_OPERATION_ENTITY_SAVE, \
    DB_OPERATION_ENTITY_GET, DB_OPERATION_ENTITY_DEL, UNSUPPORTED_DB_OPERATION
from db_store.datastore import DBStore


# TBD: Handle singleton per DB NAME

class DBAccessReq(object):  # FIXME: Move it somewehere else
    def __init__(self, entity_name, op, data, fut):
        self.entity_name = entity_name
        self.op = op
        self.op_data = data
        self.result = fut


class DBAccessResp(object):
    def __init__(self, status, result):
        self.status = status
        self.result = result


class DBStoreWorkers(object):
    def __init__(self, name, req_queue):
        self.name = name
        self.req_queue = req_queue
        self.db = DBStore(name)
        self.worker_count = None
        self.task_queue_size = MAX_TASK_QUEUE_SIZE
        self.workers = {}

    @staticmethod
    def __db_error_message(code, value):
        return json.dumps({"_error": code.format(value)})

    def __add_table(self, table_name, indexes):
        if self.db.get_table(table_name):
            return True, None
        self.db.register_table(table_name, indexes)
        return True, None

    def __add_update_object(self, table_name, content):
        table = self.db.get_table(table_name)
        if not table:
            return False, self.__db_error_message(TABLE_NOT_FOUND, table_name)

        record = None
        if "id" in content and len(content["id"]) == DEFAULT_UUID_LEN:
            record = table.get_record(content["id"])
            if not record:
                return False, self.__db_error_message(ENTITY_NOT_FOUND, content["id"])

        record = table.add_record(content, record)
        if not record:
            return False, self.__db_error_message(DUPLICATE_ENTITY_FOUND, table_name)

        return True, json.dumps(record.__dict__)

    def __get_one_or_more_object(self, table_name, filters):
        table = self.db.get_table(table_name)
        if not table:
            return False, self.__db_error_message(TABLE_NOT_FOUND, table_name)
        record_ids = set()
        records = []

        if not filters:
            records = [json.dumps(r.__dict__) for r in list(table.get_records().values())]
            return True, records

        for f, v in filters.items():
            #print(f"{f},{v}")
            indexed = table.get_indexed(f)
            if not indexed:
                # LOG
                #print("NO Indexed object found")
                continue
            #print(indexed.indexed_values)
            ids = indexed.get_indexed_record_ids(v)
            if not ids:
                # LOG
                #print("NO Ids object found")
                continue
            record_ids = record_ids.union(ids)

        #print(record_ids)
        for _id in record_ids:
            r = table.get_record(_id)
            if not r:
                return False, self.__db_error_message(ENTITY_NOT_FOUND, _id)
            records.append(json.dumps(r.__dict__))

        return True, records

    def __del_one_object(self, table_name, _id):
        table = self.db.get_table(table_name)
        if not table:
            return False, self.__db_error_message(TABLE_NOT_FOUND, table_name)
        if not table.get_record(_id):
            return False, self.__db_error_message(ENTITY_NOT_FOUND, _id)

        table.del_record(_id)

        return True, None

    async def __process_requests(self, task_queue):
        while True:
            task = await task_queue.get()
            #print(f"RECV TASK:{task.op}, {task.op_data}")
            if task.op == DB_OPERATION_CREATE_ENTITY:
                status, result = self.__add_table(task.entity_name, task.op_data)
            elif task.op == DB_OPERATION_ENTITY_SAVE:
                status, result = self.__add_update_object(task.entity_name, task.op_data)
            elif task.op == DB_OPERATION_ENTITY_GET:
                status, result = self.__get_one_or_more_object(task.entity_name, task.op_data)
            elif task.op == DB_OPERATION_ENTITY_DEL:
                status, result = self.__del_one_object(task.entity_name, task.op_data)
            else:
                status, result = False, self.__db_error_message(UNSUPPORTED_DB_OPERATION, task.op)


            task.result.set_result(DBAccessResp(status, result))

    async def run(self):
        try:
            self.worker_count = await self.req_queue.get()
            if not self.worker_count or int(self.worker_count) <= 1:
                self.worker_count = 1
                # LOG

            task_queue = asyncio.Queue(maxsize=self.task_queue_size)
            for i in range(self.worker_count):
                # LOG
                self.workers["workers_" + str(i)] = asyncio.create_task(self.__process_requests(task_queue))

            self.req_queue.task_done()

            while True:
                db_req = await self.req_queue.get()
                #print("recv req")
                if not isinstance(db_req, DBAccessReq):
                    # LOG
                    continue

                await task_queue.put(db_req)
            # LOG
        except asyncio.CancelledError:
            pass
            # LOG
        finally:
            for name, worker in self.workers.items():
                # LOG
                worker.cancel()
