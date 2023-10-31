from dataclasses import dataclass


@dataclass
class ObjectType:
    singular: str
    plural: str


class ComputePool(ObjectType):
    singular = "compute pool"
    plural = "compute pools"


class Database(ObjectType):
    singular = "database"
    plural = "databases"


class Function(ObjectType):
    singular = "function"
    plural = "functions"


class Job(ObjectType):
    singular = "job"
    plural = "jobs"


class Procedure(ObjectType):
    singular = "procedure"
    plural = "procedures"


class Role(ObjectType):
    singular = "role"
    plural = "roles"


class Schema(ObjectType):
    singular = "schema"
    plural = "schemas"


class Service(ObjectType):
    singular = "service"
    plural = "services"


class Streamlit(ObjectType):
    singular = "streamlit"
    plural = "streamlits"


class Table(ObjectType):
    singular = "table"
    plural = "tables"


class Warehouse(ObjectType):
    singular = "warehouse"
    plural = "warehouses"
