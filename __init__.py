full_cmd = re.compile('(?:(?P<command>[a-zA-Z0-9_-]+)*)\s*(?:(?P<entity>[a-zA-Z0-9_-]+)*)\s*(?:(?P<args>.+)*)')
cmd_args = re.compile('(?P<key>\w+)=(?P<value>[^\s.]+)')

DEFAULT_DB = "QuickReserve_DB"
DB_WORKER_POOL_SIZE = 4
MAX_REQ_QUEUE_SIZE = 100


