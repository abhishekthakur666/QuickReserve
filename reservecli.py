import asyncio
import cmd
import json
import threading
import signal
import logging
import sys
import re
from termcolor import colored
from collections import ChainMap
from prettytable import PrettyTable
import readline

from db_lib import base_dao
from db_lib.base_dao import DBClient
from db_store import datastore_workers
from db_store.datastore_workers import DBStoreWorkers
from models.base_dataobject import BaseDO
from models.car_resources import CarDO, CarStateDO, CarInspectStateDO
from models.operator_resources import OperatorDO, OperatorCredentialsDO

# These entities can be managed from CLI
supported_entities = {"cars": CarDO,
                      "car-reservations": CarStateDO,
                      "operators": OperatorDO,
                      "op-credentials": OperatorCredentialsDO,
                      "session": OperatorCredentialsDO,
                      "inspect-reservations": CarInspectStateDO
                      }

FULL_CMD_EXP = re.compile('(?:(?P<command>[a-zA-Z0-9_-]+)*)\s*(?:(?P<entity>[a-zA-Z0-9_-]+)*)\s*(?:(?P<args>.+)*)')
CMD_ARGS_EXP = re.compile('(?P<key>\w+)=(?P<value>[^\s.]+)')

DEFAULT_DB = "QuickReserve_DB"
DB_WORKER_POOL_SIZE = 4
MAX_REQ_QUEUE_SIZE = 100


# SIGINT handler
def signal_handler(_signal, _):
    pass


entities_meta_info_map = {}


class EntitiesMetaInfo(object):

    def __init__(self, name, attributes, indexes):
        self.name = name
        self.attributes = attributes
        self.indexes = indexes

    def __str__(self):
        return "[ " + " ".join([self.name, str(self.attributes), str(self.indexes)]) + " ]"


#####  MAIN MENU FOR ALL Entities ####
#        /              \   ######
# Operator CLI          Reservaation CLI ####

class MainMenu(cmd.Cmd):
    delimiters = readline.get_completer_delims().replace("-", "")
    readline.set_completer_delims(delimiters)

    def __init__(self, label, role, parent_label="", parent_role=""):
        super().__init__()
        self.role = role
        self.label = label
        self.parent_role = parent_role
        self.parent_label = parent_label
        self.singleton_cmds = {}
        self.entity_cmds = {"register", "modify", "show", "unregister", "query"}
        self.entities_meta_info_map = {}  # FIXME: Please rename it accordingly

        cmd.Cmd.prompt = f"{colored(self.label, 'green', attrs=['bold'])}:({colored(self.role, 'cyan', attrs=['bold'])})#"

    @staticmethod
    def parse_cmd_entity_args(line):
        m = FULL_CMD_EXP.search(line)
        command = m.group("command")
        entity = m.group("entity")
        args = m.group("args")

        if not args:
            return command, entity, None

        args = [{m.groupdict()["key"]: m.groupdict()["value"]} for m in CMD_ARGS_EXP.finditer(m.group("args"))]
        args = dict(ChainMap(*args))
        return command, entity, args

    def do_unregister(self, arg):
        command, entity, args = self.parse_cmd_entity_args("unregister " + arg)
        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities or not args or "id" not in args:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        entity_class = supported_entities[entity]
        if not entity_class.verify_authorization(self.role):
            print('Permission denied for executing this operation')
            return

        entity_class = supported_entities[entity]
        res, objects = entity_class.dao.get(args)
        if not res:
            print(f'Failed to query : {entity})')
            return

        obj = json.loads(objects[0])["content"]
        if self.label != entity_class(**obj).last_updated_by:
            print('Permission denied for executing this operation')
            return

        for d, k in entity_class.dependent_by.items():
            res, objects = d.dao.get({k: obj.get(k)})
            if res and objects:
                print(f'Instances of dependent entity:{d.__name__} is dependent on {entity_class.__name__}')
                return

        res, obj = entity_class.dao.remove(args["id"])
        if not res:
            print(f'Failed to Delete : {entity}: reason:{json.loads(obj)["_error"]}')
            return

        print(f'{entity} with id:{args["id"]} unregistered successfully')
        self.lastcmd = ""

    def do_show(self, arg):
        command, entity, args = self.parse_cmd_entity_args("show " + arg)
        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        if args and not set(list(args.keys())).issubset(set(list(self.entities_meta_info_map[entity].indexes.keys()))):
            print(f"Unsupported attributes provided for querying :{entity}")
            return

        entity_class = supported_entities[entity]
        res, objects = entity_class.dao.get(args)
        if not res:
            print(f'Failed to query : {entity})')
            return

        if not objects:
            print(f'No instances of {entity} is registered in system')
            return

        t = PrettyTable(['key', 'value'])

        for obj in objects:
            for key, val in json.loads(obj)["content"].items():
                t.add_row([key, val])
            t.add_row(["\n\n", "\n\n"])
        print(t)

        self.lastcmd = ""

    def validate_input(self, entity_meta_info, args):
        if not set(list(entity_meta_info.indexes.keys())).issubset(set(list(args.keys()))):
            print("Incomplete command - Please provide all mandatory parameters for registering entity")
            print(f"Expected:{set(list(entity_meta_info.indexes.keys()))}")
            print(f"Given:{set(list(set(list(args.keys()))))}")
            return False

        if not set(list(args.keys())).issubset(set(entity_meta_info.attributes)):
            print(f"Unsupported attributes provided for registering a new entity")
            print(f"Expected:{set(entity_meta_info.attributes)}")
            print(f"Given:{set(list(set(list(args.keys()))))}")
            return False

        return True

    def do_modify(self, arg):
        command, entity, args = self.parse_cmd_entity_args("modify " + arg)
        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in entities or not args:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        entity_class = supported_entities[entity]
        if not entity_class.verify_authorization(self.role):
            print('Permission denied for executing this operation')
            return

        if not set(list(args.keys())).issubset(set(self.entities_meta_info_map[entity].attributes)):
            print(f"Unsupported attributes provided for modification of :{entity}")
            return

        relations = entity_class.relations
        for k, e in (relations or {}).items():
            res, related_entity = e.dao.get({k: args.get(k, "")})
            if not res or not related_entity or json.loads(related_entity[0])["content"].get(k) != args.get(k):
                print(f"{e.__name__} with {k}={args.get(k)} does not exist")
                return

        res, objects = entity_class.dao.get(args)
        if not res:
            print(f'Failed to query : {entity}')
            return

        if len(objects) > 1:
            print(f'Internal server error:Duplicate entities with same unique key found')
            return

        entity_class = supported_entities[entity]
        old = json.loads(objects[0])["content"]
        old_obj = entity_class(old)
        merge_content = {**old, **args, "last_updated_by": self.label}
        final_obj = entity_class(**merge_content)
        status, reason = old_obj.validate(final_obj)
        if not status:
            print(reason)
            return

        res, obj = entity_class.dao.save(final_obj.__dict__)
        if not res:
            print(f'Failed to modify : {entity} with id:{arg["id"]}: reason:{json.loads(obj)["_error"]}')
            return

        t = PrettyTable(['key', 'value'])
        for key, val in json.loads(obj)["content"].items():
            t.add_row([key, val])
        print(t)
        self.lastcmd = ""

    def default(self, line):
        print("Unsupported command - Please try with supported options")

    def do_register(self, arg):
        command, entity, args = self.parse_cmd_entity_args("register " + arg)

        entities = list(self.entities_meta_info_map.keys())
        if not entity or entity not in self.entities_meta_info_map.keys() or not args:
            print("Incomplete command - Please use autocomplete(tab) to check for supported options")
            return

        entity_class = supported_entities[entity]
        if not entity_class.verify_authorization(self.role):
            print('Permission denied for executing this operation')
            return

        if not self.validate_input(self.entities_meta_info_map[entity], args):
            return

        relations = entity_class.relations
        for k, e in (relations or {}).items():
            res, related_entity = e.dao.get({k: args.get(k, "")})
            if not res or not related_entity:
                print(f"{e.__name__} with {k}={args.get(k)} does not exist")
                return

        args["last_updated_by"] = self.label
        obj = entity_class(**args)
        status, reason = obj.validate()
        if not status:
            print(reason)
            return

        res, obj = entity_class.dao.save(obj.__dict__)
        if not res:
            print(f'Failed to register new  {entity}- reason:{json.loads(obj)["_error"]}')
            return

        t = PrettyTable(['key', 'value'])
        for key, val in json.loads(obj)["content"].items():
            t.add_row([key, val])
        print(t)
        self.lastcmd = ""

    # Function is used to autocomplete command based on args
    def completedefault(self, text, line, begidx, endidx):
        logger.debug(f"INPUT LINE-{line}, {text}")
        command, entity, args = self.parse_cmd_entity_args(line)
        logger.debug(f"AFTER PARSE-{command}, {entity}, {args}")
        if command not in self.entity_cmds and command not in self.singleton_cmds:
            return []

        if command in self.singleton_cmds:
            entity = ""

        filter_text = text
        if not args:
            args = {}

        if not entity:
            if command in self.singleton_cmds:
                return [attr + "=" for attr in self.singleton_cmds[command].attributes if
                        attr.startswith(filter_text) and attr not in list(args.keys())]
            logger.info(list(self.entities_meta_info_map.keys()))
            return list(self.entities_meta_info_map.keys())

        entities = list(self.entities_meta_info_map.keys())
        if entity not in entities:
            return [e for e in entities if e.startswith(entity)]

        filter_text = ""
        if text != entity:
            filter_text = text

        if not args:
            args = {}

        attrs = self.entities_meta_info_map[entity].attributes
        if command == "unregister":
            attrs = ["id"]

        return [attr + "=" for attr in attrs if
                attr.startswith(filter_text) and attr not in list(args.keys())]

    def do_exit(self, _):
        if self.parent_label and self.parent_role:
            cmd.Cmd.prompt = f"{self.parent_label}:{self.parent_role}#"
        return True

    def do_EOF(self, _):
        print("Please use exit command to exit from shell")


class ReservationMenu(MainMenu):
    def __init__(self, label, role, parent_label="", parent_role=""):
        super().__init__(label, role, parent_label, parent_role)
        self.singleton_cmds = {"inspect_reservations": entities_meta_info_map["inspect-reservations"]}
        self.entities_meta_info_map = {"cars": entities_meta_info_map["cars"],
                                       "car-reservations": entities_meta_info_map["car-reservations"]}

    def do_inspect_reservation(self, arg):
        command, entity, args = self.parse_cmd_entity_args("inspect_reservations singleton_entity " + arg)
        if entity != "singleton_entity":
            print("Incorrect command specified - Please use autocomplete for help")
            return

        model_based_filter = set()
        if args and args.get('model_name'):
            res, objects = CarDO.dao.get({'model_name': args.get('model_name')})
            if not res:
                print(f"Internal server error, please try after sometime !!")
                return

            if not objects:
                print(f'No instances of car is registered in system with model:{args["model_name"]}')
                return

            for obj in objects:
                model_based_filter.add(json.loads(obj)["content"]["reg_no"])

        entity_class = supported_entities["car-reservations"]
        res, objects = entity_class.dao.get({})
        if not res:
            print(f"Internal server error, please try after sometime !!")
            return

        if not objects:
            print(f'No instances of car-reservation is registered in system')
            return

        found = False
        t = PrettyTable(['key', 'value'])
        for obj in objects:
            reservation_obj = json.loads(obj)["content"]
            if model_based_filter and reservation_obj["reg_no"] not in model_based_filter:
                continue
            found = True
            for key, val in reservation_obj.items():
                t.add_row([key, val])
            t.add_row(["\n\n", "\n\n"])

        if not found:
            print(f'No car reservation instances found for model:{args.get("model_name")}')
            return

        print(t)
        self.lastcmd = ""


class OperatorMenu(MainMenu):
    def __init__(self, label, role, parent_label="", parent_role=""):
        super().__init__(label, role, parent_label, parent_role)
        self.singleton_cmds = {"login": entities_meta_info_map["session"]}
        self.entities_meta_info_map = {"operators": entities_meta_info_map["operators"],
                                       "op-credentials": entities_meta_info_map["op-credentials"]}

    def do_login(self, arg):
        command, entity, args = self.parse_cmd_entity_args("login singleton_entity " + arg)
        if entity != "singleton_entity" or not args or not args.get("email_address") or not args.get("password"):
            print("Incomplete command - Please provide all mandatory parameters for operator login")
            return

        entity_meta_info = self.singleton_cmds["login"]
        if not self.validate_input(entity_meta_info, args):
            return

        entity_class = supported_entities["operators"]
        res, objects = entity_class.dao.get({"email_address": args["email_address"]})
        if not res or not len(objects):
            print(f'Failed to fetch operator')
            return

        op = OperatorDO(**(json.loads(objects[0])["content"]))
        entity_class = supported_entities["op-credentials"]
        res, objects = entity_class.dao.get({"email_address": op.email_address})
        if not res or not len(objects):
            print(f'Failed to fetch operator credentials')
            return

        op_cred = OperatorCredentialsDO(**(json.loads(objects[0])["content"]))
        entered_cred = OperatorCredentialsDO(email_address=op.email_address, password=args["password"])
        if op_cred.password != entered_cred.password:
            print(f'Invalid credential for operator:{args["email_address"]}')
            return

        ReservationMenu(label=op.email_address, role=op.role, parent_label=self.label, parent_role=self.role).cmdloop()
        self.lastcmd = ""


def setup_entities_metadata(entities):
    parent = BaseDO()
    for e in entities:
        if e not in supported_entities:
            logger.error(f"Entity: {e} is not supported")
            continue

        logger.info(f"Registered entity: {e} for processing")
        obj = supported_entities[e]()
        attrs = list(set(list(obj.__dict__.keys())) - set(list(parent.__dict__.keys())))
        entities_meta_info_map[e] = EntitiesMetaInfo(e, attrs,
                                                     obj.dao.indexes.copy() if hasattr(obj.dao, "indexes") else {})


# ENTRY POINT for EVEN LOOP FOR HANDLING DB REQUEST FRO CLIENTS / CLI
async def ev_loop_main(entities):
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
    asyncio.run(ev_loop_main(entities))


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)
    log_level = logging.INFO
    if len(sys.argv) > 1 and sys.argv[1] == '-D':
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level, filename='quick_reserve.log', filemode='w',
                        format='%(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger()
    clilogger = logging.getLogger()
    clilogger.setLevel(logging.INFO)
    base_dao.logger = datastore_workers.logger = logger  # FIXME: Find better way using custom logger and module level logging support
    setup_event = threading.Event()
    threading.Thread(target=start_ev_loop, args=(list(supported_entities.keys()),), daemon=True).start()
    setup_event.wait()  # Event thread is successfully initialized, now start cli

    OperatorMenu("abhishek@qr.com", "master").cmdloop()
