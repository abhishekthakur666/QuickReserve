import uuid

# TBD: Add locks while accessing database
# TBD: Compress the data


class DBStore(object):
    def __init__(self, name):
        self.name = name
        self.tables = {}

    def register_table(self, table_name, indexes=None):
        if table_name in self.tables:
            return
        ts = TableStore(table_name)
        self.tables[table_name] = ts
        if not indexes:
            indexes = {"id": True}
        else:
            indexes["id"] = True

        for index, unique in indexes.items():
            ts.register_index(index, unique)

    def drop_table(self, table_name):
        if table_name not in self.tables:
            return
        tb = self.tables[table_name]
        if not isinstance(tb, table_name):
            del self.tables[table_name]
            return

        indexes = tb.keys() or []
        for index in indexes:
            tb.del_index(index)

    def get_table(self, table_name):
        return self.tables.get(table_name, None)

    def get_tables(self):
        return self.tables


class TableStore(object):
    def __init__(self, name):
        self.name = name
        self.indexes = {}
        self.records = {}

    def register_index(self, index_name, is_unique):
        if index_name in self.indexes:
            return
        self.indexes[index_name] = IndexStore(index_name, is_unique)

    def del_index(self, index_name):
        if index_name not in self.indexes:
            return
        del self.indexes[index_name]

    def get_indexed(self, index_name):
        return self.indexes.get(index_name, None)

    def add_record(self, content, record=None):
        # TBD: Handle unique values
        if not record:
            record = Record(content)
        else:
            content["created_at"] = record.content["created_at"]
            record.content = content
        record.content["id"] = record.id

        for i, o in self.indexes.items():
            if not isinstance(o, IndexStore):
                continue
            if not o.validate_uniqueness(content[i], record.id):
                return None

        self.records[record.id] = record
        for i, o in self.indexes.items():
            if not isinstance(o, IndexStore):
                continue
            o.register_indexed_record_id(content[i], record.id)
        return record

    def del_record(self, record_id):
        if record_id not in self.records:
            return
        for i, o in self.indexes.items():
            if not isinstance(o, IndexStore):
                continue
            o.del_indexed_record_id(i, record_id)

        del self.records[record_id]

    def get_record(self, record_id):
        return self.records.get(record_id)

    def get_records(self):
        return self.records


class IndexStore(object):
    def __init__(self, name, is_unique):
        self.name = name
        self.is_unique = is_unique
        self.indexed_values = {}

    def validate_uniqueness(self, value, record_id):
        if not self.is_unique:
            return True
        record_ids = self.indexed_values.get(value)
        if not record_ids or len(record_ids - {record_id}) == 0:
            return True
        return False

    def register_indexed_record_id(self, value, record_id):
        if not self.indexed_values.get(value):
            self.indexed_values[value] = set()
        if record_id in self.indexed_values[value]:
            return
        self.indexed_values[value].add(record_id)

    def get_indexed_record_ids(self, value):
        return self.indexed_values.get(value)

    def del_indexed_record_id(self, value, record_id):
        if not self.indexed_values.get(value) or \
                record_id not in self.indexed_values[value]:
            return
        self.indexed_values[value].remove(record_id)


class Record(object):
    """ Represent a physical record of an entity
        having unique system generated id
    """

    def __init__(self, content):
        self.id = str(uuid.uuid4())
        self.content = content
