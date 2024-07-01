from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, orm
import json

with open("connect.json", encoding="utf-8") as file:
    data = json.load(file)
    name = data["name"]
    password = data["password"]
    host = data["host"]
    port = data["port"]
    db_name = data["db_name"]

DSN = f'postgresql://{name}:{password}@{host}:{port}/{db_name}'
engine = create_engine(DSN)

Session = orm.sessionmaker(bind=engine)
session = Session()

metadata = MetaData()
metadata.drop_all(engine)

n_model = dict()
with open("tests_data.json", encoding="utf-8") as file:
    data = json.load(file)
    for d in data:
        if d["model"] not in n_model:
            for k, v in d["fields"].items():
                if isinstance(v, int):
                    if d["model"] not in n_model:
                        n_model[d["model"]] = {k: Integer}
                    else:
                        n_model[d["model"]].update({k: Integer})
                else:
                    if d["model"] not in n_model:
                        n_model[d["model"]] = {k: String}
                    else:
                        n_model[d["model"]].update({k: String})


for m in n_model:
    table_name = m
    columns = [Column("id", Integer, primary_key=True)] + [Column(k, v, nullable=True) for k, v in n_model[m].items()]

    dynamic_table = Table(table_name, metadata, *columns) #Создаем новую таблицу с динамически созданными колонками

model = dict()
with open("tests_data.json", encoding="utf-8") as file:
    data = json.load(file)
    for d in data:
        if d["model"] not in model:
            model[d["model"]] = {d["pk"]: d["fields"]}
        else:
            model[d["model"]].update({d["pk"]: d["fields"]})
for tab_name, v in model.items():
    print(tab_name, v)
    # for key_id, v1 in v.items():
    #     print(f"table: {tab_name}, id: {key_id}, {v1}")

        # for field_name, val in v1.items():
        #     print(f"table: {tab_name}, id: {key_id}, field: {field_name}, value: {val}")

session.close()