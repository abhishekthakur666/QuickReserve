# Operation for Accessing Database.This allows us to define protocol between db client and db server.
DB_OPERATION_CREATE_ENTITY = 1
DB_OPERATION_DROP_ENTITY = 2
DB_OPERATION_ENTITY_GET = 3
DB_OPERATION_ENTITY_SAVE = 4
DB_OPERATION_ENTITY_DEL = 5

# ERROR Messages returned by DB server
TABLE_NOT_FOUND = "Table {} does not exist"
ENTITY_NOT_FOUND = "Entity with id : {} does not exist"
DUPLICATE_ENTITY_FOUND = "Entity: {} information overlap with other entities"
UNSUPPORTED_DB_OPERATION = "DB Operation: {} is not supported"

# constants to be used by DB Server
MAX_TASK_QUEUE_SIZE = 100
DEFAULT_UUID_LEN = 36
