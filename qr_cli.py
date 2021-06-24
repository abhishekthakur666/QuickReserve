import asyncio
import cmd
import json

import threading
import signal
import logging
import time

from collections import ChainMap
import datetime

from prettytable import PrettyTable
import readline
import re
from db_lib.base_dao import DBClient
from db_store.datastore_workers import DBStoreWorkers
from reserve_entities.car_do import CarDO, CarStateDO
from reserve_entities.login_state_do import LoginStateDO
from reserve_entities.operator_do import OperatorDO, OperatorCredentialsDO

full_cmd = re.compile('(?:(?P<command>[a-zA-Z0-9_-]+)*)\s*(?:(?P<entity>[a-zA-Z0-9_-]+)*)\s*(?:(?P<args>.+)*)')
cmd_args = re.compile('(?P<key>\w+)=(?P<value>[^\s.]+)')  # FIXME: Handle for last arg

DEFAULT_DB = "QuickReserve_DB"
DB_WORKER_POOL_SIZE = 4
MAX_REQ_QUEUE_SIZE = 100

supported_entities = {"car": CarDO, "car-reservation": CarStateDO,
                      "operator": OperatorDO,
                      "op_credentials": OperatorCredentialsDO,
                      "session": LoginStateDO}

entities_meta_info_map = {}


class EntitiesMetaInfo(object):

    def __init__(self, name, attributes, indexes):
        self.name = name
        self.attributes = attributes
        self.indexes = indexes

    def __str__(self):
        return "[ " + " ".join([self.name, str(self.attributes), str(self.indexes)]) + " ]"


# SIGINT handler
def signal_handler(signal, frame):
    pass


# Set logging to use function name
logger = logging.getLogger(__name__)


class MainMenu(cmd.Cmd):
    delimiters = readline.get_completer_delims().replace("-", "")
    readline.set_completer_delims(delimiters)

    def __init__(self, role, label, parent_role="", parent_label=""):
        super().__init__()
        self.role = role
        self.label = label
        self.parent_role = parent_role
        self.parent_label = parent_label
        self.supported_cmds = set(["register", "modify", "show", "delete"])
        self.entities_meta_info_map = {"car": entities_meta_info_map["car"]}

        cmd.Cmd.prompt = f"{self.label}:{self.role}#"

    @staticmethod
    def parse_cmd_entity_args(line):
        m = full_cmd.search(line)
        cmd = m.group("command")
        entity = m.group("entity")
        args = m.group("args")

        if not args:
            return cmd, entity, None

        args = [{m.groupdict()["key"]: m.groupdict()["value"]} for m in cmd_args.finditer(m.group("args"))]
        args = dict(ChainMap(*args))
        return cmd, entity, args

    def do_delete(self, arg):
        cmd, entity, args = self.parse_cmd_entity_args("delete " + arg)
        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities or not args or "id" not in args:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        entity_class = supported_entities[entity]
        res, objects = entity_class.dao.remove(args["id"])
        if not res:
            print(f'Failed to Delete : {entity}: reason:{json.loads(obj["_error"])}')
            return

        print(f'{entity} with id:{args["id"]} deleted successfully')
        self.lastcmd = None

    def do_reserve(self, arg):
        pass

    def do_show(self, arg):
        cmd, entity, args = self.parse_cmd_entity_args("show " + arg)
        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        # if not set(list(self.entities_meta_info_map[entity].indexes.keys())).issubset(set(list(args.keys()))):
        #    print("Incomplete command - Please provide all mandatory parameters for registering entity")
        #    # print(f"Expected:{set(list(self.entities_meta_info_map[entity].indexes.keys()))}")
        #    # print(f"Given:{set(list(set(list(args.keys()))))}")
        #    return

        if args and not set(list(args.keys())).issubset(set(list(self.entities_meta_info_map[entity].indexes.keys()))):
            print(f"Unsupported attributes provided for querying :{entity}")
            return

        logger.info(f"ARGS:{args}:type:{type(args)}")
        entity_class = supported_entities[entity]
        res, objects = entity_class.dao.get(args)
        if not res:
            print(f'Failed to query : {entity})')
            return

        if not objects:
            print(f'No instances of {entity} is registered in system')
            return

        # print(objects)
        t = PrettyTable(['key', 'value'])

        for obj in objects:
            # print(obj)
            for key, val in json.loads(obj)["content"].items():
                t.add_row([key, val])
            t.add_row(["\n\n", "\n\n"])
        print(t)

        self.lastcmd = None

    def do_modify(self, arg):
        cmd, entity, args = self.parse_cmd_entity_args("modify " + arg)
        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities or not args or "id" not in args:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        if not set(list(args.keys())).issubset(set(self.entities_meta_info_map[entity].attributes)):
            print(f"Unsupported attributes provided for modification of :{entity}")
            return

        logger.info(f"ARGS:{args}:type:{type(args)}")
        entity_class = supported_entities[entity]
        res, objects = entity_class.dao.get(args)
        if not res:
            print(f'Failed to query : {entity}')
            return

        if len(objects) > 1:
            print(f'Internal server error:Duplicate entities with same id:{args["id"]} found')
            return

        old = json.loads(objects[0])["content"]
        final_obj = {**old, **args}
        logger.info(f"ARGS:{args}:type:{type(args)}")
        entity_class = supported_entities[entity]
        res, obj = entity_class.dao.save(entity_class(**final_obj).__dict__)
        if not res:
            print(f'Failed to modify : {entity} with id:{arg["id"]}: reason:{json.loads(obj["_error"])}')
            return

        t = PrettyTable(['key', 'value'])
        for key, val in json.loads(obj)["content"].items():
            t.add_row([key, val])
        print(t)
        self.lastcmd = None

    def default(self, line):
        print("Unsupported command - Please try with supported options")

    def do_register(self, arg):
        cmd, entity, args = self.parse_cmd_entity_args("register " + arg)

        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities or not args:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        if not set(list(self.entities_meta_info_map[entity].indexes.keys())).issubset(set(list(args.keys()))):
            print("Incomplete command - Please provide all mandatory parameters for registering entity")
            print(f"Expected:{set(list(self.entities_meta_info_map[entity].indexes.keys()))}")
            print(f"Given:{set(list(set(list(args.keys()))))}")
            return

        if not set(list(args.keys())).issubset(set(self.entities_meta_info_map[entity].attributes)):
            print(f"Unsupported attributes provided for registering a new :{entity}")
            print(f"Expected:{set(self.entities_meta_info_map[entity].attributes)}")
            print(f"Given:{set(list(set(list(args.keys()))))}")
            return

        logger.info(f"ARGS:{args}:type:{type(args)}")
        entity_class = supported_entities[entity]
        res, obj = entity_class.dao.save(entity_class(**args).__dict__)
        if not res:
            print(f'Failed to register new : {entity} : reason:{json.loads(obj["_error"])}')
            return

        t = PrettyTable(['key', 'value'])
        for key, val in json.loads(obj)["content"].items():
            t.add_row([key, val])
        print(t)
        self.lastcmd = None

    def completedefault(self, text, line, begidx, endidx):
        logger.info(f"LINE-{line}, {text}")
        cmd, entity, args = self.parse_cmd_entity_args(line)
        logger.info(f"AFTER PARSE-{cmd}, {entity}, {args}")
        if cmd not in self.supported_cmds:
            return []

        if not entity:
            logger.info(list(self.entities_meta_info_map.keys()))
            return list(self.entities_meta_info_map.keys())

        entities = list(self.entities_meta_info_map.keys())
        if entity not in entities:
            for e in entities:
                return [e for e in entities if e.startswith(entity)]

        filter = ""
        if text != entity:
            filter = text

        if not args:
            args = {}

        return [attr + "=" for attr in self.entities_meta_info_map[entity].attributes if
                attr.startswith(filter) and attr not in list(args.keys())]

    def do_exit(self, arg):
        if self.parent_label and self.parent_role:
            cmd.Cmd.prompt = f"{self.parent_label}:{self.parent_role}#"
        return True

    def do_EOF(self, arg):
        print("Please use exit command to exit from shell")


class ReservationMenu(MainMenu):
    def __init__(self, role, label, parent_role="", parent_label=""):
        super().__init__(role, label, parent_role, parent_label)
        self.supported_cmds = set(["reserve", "register", "modify", "show", "delete"])
        self.entities_meta_info_map = {"car": entities_meta_info_map["car"],
                                       "car-reservation": entities_meta_info_map["car-reservation"]}

    def do_reserve(self, arg):
        cmd, entity, args = self.parse_cmd_entity_args("reserve car-reservation " + arg)
        if entity != "car-reservation" or not args or not args.get("car_reg_no"):
            print("Incomplete command - Please provide all mandatory parameters for reserving command")

        entity_class = supported_entities["car"]
        res, objects = entity_class.dao.get({"reg_no": args["car_reg_no"]})
        if not res or not len(objects):
            print(f'Failed to fetch Car with reg_no:{args["car_reg_no"]}')
            return

        res, objects = supported_entities["car-reservation"].dao.get({"car_reg_no": args["car_reg_no"]})
        if not res:
            print(f"Internal server error, please try after sometime !!")
            return

        if len(objects):
            current_datetime = datetime.datetime.now().strftime("%d/%m/%YT%H:%M:%S")
            car_resv = CarStateDO(**(json.loads(objects[0])["content"]))
            # print(f'BOOKED:{time.strptime(car_resv.booked_till, "%d/%m/%YT%H:%M:%S")}')
            # print(f'CURRENT:{time.strptime(current_datetime,"%d/%m/%YT%H:%M:%S")}')
            if time.strptime(car_resv.booked_till, "%d/%m/%YT%H:%M:%S") >= \
                    time.strptime(current_datetime, "%d/%m/%YT%H:%M:%S"):
                print(f'Car with reg_no:{args["car_reg_no"]} is already reserved till:{car_resv.booked_till}')
                return False

        if "booked_by" not in args:
            args["booked_by"] = self.label
        if "booked_till" not in args:
            args["booked_till"] = CarStateDO.get_datetime_till_booked().strftime("%d/%m/%YT%H:%M:%S")
        entity_class = supported_entities["car-reservation"]
        res, obj = entity_class.dao.save(entity_class(**args).__dict__)
        if not res:
            print(f'Failed to reserve : {entity} : reason:{json.loads(obj["_error"])}')
            return

        t = PrettyTable(['key', 'value'])
        for key, val in json.loads(obj)["content"].items():
            t.add_row([key, val])
        print(t)
        self.lastcmd = None

    def completedefault(self, text, line, begidx, endidx):
        logger.info(f'BEFORE_PARSE:{line}')
        if line and line.startswith("reserve"):
            line = line.replace("reserve ", "reserve car-reservation ")
        #    print(line)

        logger.info(f"LINE-{line}, {text}")

        cmd, entity, args = self.parse_cmd_entity_args(line)
        logger.info(f"AFTER PARSE-{cmd}, {entity}, {args}")
        if cmd not in self.supported_cmds:
            return []

        ent_list = list(self.entities_meta_info_map.keys())
        if not entity:
            logger.info(ent_list)
            ent_list.remove("car-reservation")
            return ent_list

        if entity not in ent_list:
            ent_list.remove("car-reservation")
            for e in ent_list:
                return [e for e in ent_list if e.startswith(entity)]

        filter_text = ""
        if text != entity:
            filter_text = text

        if not args:
            args = {}

        return [attr + "=" for attr in self.entities_meta_info_map[entity].attributes if
                attr.startswith(filter_text) and attr not in list(args.keys())]


class OperatorMenu(MainMenu):
    def __init__(self, role, label, parent_role="", parent_label=""):
        super().__init__(role, label, parent_role, parent_label)
        self.supported_cmds = set(["login", "register", "modify", "show", "delete"])
        self.entities_meta_info_map = {"operator": entities_meta_info_map["operator"],
                                       "op_credentials": entities_meta_info_map["op_credentials"],
                                       "session": entities_meta_info_map["session"]}

    def do_login(self, arg):
        cmd, entity, args = self.parse_cmd_entity_args("login session " + arg)
        if entity != "session" or not args or not args.get("email_address") or not args.get("password"):
            print("Incomplete command - Please provide all mandatory parameters for operator login")

        entity_class = supported_entities["operator"]
        res, objects = entity_class.dao.get({"email_address": args["email_address"]})
        if not res or not len(objects):
            print(f'Failed to fetch operator')
            return

        # print(json.loads(objects[0])["content"])
        op = OperatorDO(**(json.loads(objects[0])["content"]))
        entity_class = supported_entities["op_credentials"]
        res, objects = entity_class.dao.get({"operator_email": op.email_address})
        if not res or not len(objects):
            print(f'Failed to fetch operator credentials')
            return

        op_cred = OperatorCredentialsDO(**(json.loads(objects[0])["content"]))
        entered_cred = OperatorCredentialsDO(operator_email=op.email_address, password=args["password"])
        # print(f"OP_CRED:{op_cred.password}")
        # print(f"ENTE_CRED:{entered_cred.password}")
        if op_cred.password != entered_cred.password:
            print(f'Invalid credential for operator:{args["email_address"]}')
            return

        ReservationMenu(role=op.role, label=op.email_address, parent_role=self.role, parent_label=self.label).cmdloop()
        self.lastcmd = None

    def completedefault(self, text, line, begidx, endidx):
        logger.info(f'BEFORE_PARSE:{line}')
        if line and line.startswith("login"):
            line = line.replace("login ", "login session ")
        #    print(line)

        logger.info(f"LINE-{line}, {text}")

        cmd, entity, args = self.parse_cmd_entity_args(line)
        logger.info(f"AFTER PARSE-{cmd}, {entity}, {args}")
        if cmd not in self.supported_cmds:
            return []

        ent_list = list(self.entities_meta_info_map.keys())
        if not entity:
            logger.info(ent_list)
            ent_list.remove("session")
            return ent_list

        if entity not in ent_list:
            ent_list.remove("session")
            for e in ent_list:
                return [e for e in ent_list if e.startswith(entity)]

        filter_text = ""
        if text != entity:
            filter_text = text

        if not args:
            args = {}

        return [attr + "=" for attr in self.entities_meta_info_map[entity].attributes if
                attr.startswith(filter_text) and attr not in list(args.keys())]


def setup_entities_metadata(entities):
    for e in entities:
        if e not in supported_entities:
            # LOG
            continue
        # LOG
        obj = supported_entities[e]()
        attrs = list(obj.__dict__.keys())
        entities_meta_info_map[e] = EntitiesMetaInfo(e, attrs, obj.dao.indexes.copy())


async def main(entities):
    loop = asyncio.get_running_loop()
    req_queue = asyncio.Queue(MAX_REQ_QUEUE_SIZE)

    DBClient(req_queue, loop)
    db_server = DBStoreWorkers(DEFAULT_DB, req_queue)
    server_worker = asyncio.create_task(db_server.run())
    await req_queue.put(DB_WORKER_POOL_SIZE)
    await req_queue.join()  # All workers are initialized correctly
    setup_entities_metadata(entities)
    setup_event.set()
    await server_worker


def start_ev_loop(entities):
    asyncio.run(main(entities))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    logging.basicConfig(level=logging.INFO, filename='app.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')
    clilogger = logging.getLogger()
    clilogger.setLevel(logging.INFO)
    setup_event = threading.Event()
    threading.Thread(target=start_ev_loop, args=(list(supported_entities.keys()),), daemon=True).start()
    setup_event.wait()
    OperatorMenu("abhishek@qr.com", "master").cmdloop()
