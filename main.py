import hashlib
import time
import duckdb

from faker import Faker
from fastapi import FastAPI
from typing import Union
from model import UserModel
from model import QueryModel
from model import ResponseModel

app = FastAPI()
db_path = "user.db"
fake = Faker(['zh_CN'])


@app.get("/status")
async def status():
    return {"status": "OK"}


@app.post("/user/create")
async def create_user(user: UserModel):
    if len(user.user_name) == 0:
        return ResponseModel(code=20001, msg="The format with field user_name is incorrect")
    if len(user.password) == 0:
        return ResponseModel(code=20002, msg="The format with field password is incorrect")
    if len(user.name) == 0:
        return ResponseModel(code=20003, msg="The format with field name is incorrect")

    user_id = fake.pyint(min_value=10000, max_value=999999999)

    current_time = int(time.time())

    password = hashlib.sha256(user.password.encode()).hexdigest()

    with duckdb.connect(db_path) as con:
        sql_add = f"INSERT INTO user (user_id, user_name, password, name, ssn, address, province, city, phone, job," \
                  f" sex, mail, birthday, is_active, is_delete, last_time) VALUES ({user_id}, '{user.user_name}'," \
                  f" '{password}', '{user.name}', '{user.ssn}', '{user.address}', '{user.province}', '{user.city}'," \
                  f" '{user.phone}', '{user.job}', '{user.sex}', '{user.mail}', '{user.birthday}', 0, 0, {current_time})"
        con.execute(sql_add)

    return ResponseModel(code=0, msg="Success", data={'user_id': user_id})


@app.post("/user/read")
async def read_user(query: Union[QueryModel, None] = None):
    result = list([])
    with duckdb.connect(db_path) as con:
        sql_base = "select user_id, user_name, name, ssn, address, province, city, phone, job, sex, mail, birthday," \
                   " is_active, is_delete, last_time from user where is_active=0 and is_delete=0"

        if query is None:
            con.execute(sql_base)
        elif query.user_id is not None and query.user_id > 0:
            sql = f"{sql_base} and user_id=?"
            try:
                con.execute(sql, [query.user_id])
            except duckdb.duckdb.CatalogException as ex:
                return ResponseModel(code=300001, msg=str(ex))
        else:
            sql_extra = ""
            params_data = list([])

            filed_value_data = dict({
                "user_name": query.user_name,
                "name": query.name,
                "ssn": query.ssn,
                "province": query.province,
                "city": query.city,
                "phone": query.phone,
                "sex": query.sex,
            })

            for k, v in filed_value_data.items():
                if v is not None and len(v) > 0:
                    sql_extra = f"{sql_extra} and {k}=?"
                    params_data.append(v)

            try:
                if len(params_data) > 0:
                    con.execute(f"{sql_base} {sql_extra}", params_data)
                else:
                    con.execute(sql_base)
            except duckdb.duckdb.CatalogException as ex:
                return ResponseModel(code=300001, msg=str(ex))

        for info in con.fetchall():
            user = UserModel(
                user_id=info[0],
                user_name=info[1],
                name=info[2],
                ssn=info[3],
                address=info[4],
                province=info[5],
                city=info[6],
                phone=info[7],
                job=info[8],
                sex=info[9],
                mail=info[10],
                birthday=info[11],
                is_active=info[12],
                is_delete=info[13],
                last_time=info[14],
            )
            result.append(user)
    if len(result) == 0:
        return ResponseModel(code=0, msg='No data')

    return ResponseModel(code=0, msg='Success', data=result)


@app.post("/user/update/{user_id}")
async def update_user(user_id: int, user: UserModel):
    if user_id <= 0:
        return ResponseModel(code=40000, msg="The format with field user_id is incorrect")

    filed_value_dict = dict({
        'user_name': user.user_name,
        'password': user.password,
        'name': user.name,
        'ssn': user.ssn,
        'birthday': user.birthday,
        'address': user.address,
        'province': user.province,
        'city': user.city,
        'phone': user.phone,
        'job': user.job,
        'sex': user.sex,
        'mail': user.mail,
        'is_active': user.is_active,
        'is_delete': user.is_delete,
    })

    sql_extra = ""
    params_data = list([])
    for k, v in filed_value_dict.items():
        if v is not None:
            if isinstance(v, str) and len(v) > 0:
                sql_extra = f"{sql_extra}, {k}=?"
                params_data.append(v)
            if isinstance(v, int):
                sql_extra = f"{sql_extra}, {k}=?"
                params_data.append(v)

    sql_extra = f"{sql_extra}, last_time=?"
    params_data.append(int(time.time()))

    sql_extra = sql_extra.lstrip(",")

    with duckdb.connect(db_path) as con:
        try:
            sql = f"UPDATE user SET {sql_extra} where user_id=?"
            params_data.append(user_id)
            con.execute(sql, params_data)
        except duckdb.duckdb.ConstraintException as ex:
            return ResponseModel(code=40001, msg=str(ex))

    return ResponseModel(code=0, msg="Success", data={'user_id': user_id})


@app.post("/user/delete/{user_id}")
async def delete_user(user_id: int):
    if user_id <= 0:
        return ResponseModel(code=50000, msg="The format with field user_id is incorrect")

    with duckdb.connect(db_path) as con:
        try:
            con.execute("DELETE From user where user_id=?", [user_id])
        except duckdb.duckdb.ConstraintException as ex:
            return ResponseModel(code=40001, msg=str(ex))

    return ResponseModel(code=0, msg="Success")


@app.get("/data/generate/{count}")
async def generate_data(count: int):
    if count <= 0:
        return ResponseModel(code=60000, msg='The format with field count is incorrect')
    with duckdb.connect(db_path) as con:
        sql = """
        CREATE TABLE user (
            user_id INTEGER,
            user_name VARCHAR,
            password VARCHAR,
            name VARCHAR,
            ssn VARCHAR,
            address VARCHAR,
            province VARCHAR,
            city VARCHAR,
            phone VARCHAR,
            job VARCHAR,
            sex VARCHAR,
            mail VARCHAR,
            birthday VARCHAR,
            is_active INTEGER,
            is_delete INTEGER,
            last_time INTEGER,
            PRIMARY KEY(user_id, user_name)
        )

        """
        try:
            con.sql(sql)
        except duckdb.duckdb.CatalogException:
            return ResponseModel(code=60001, msg="Table already exists!")

        for _ in range(count):
            profile = fake.profile()
            user_id = fake.pyint(min_value=10000, max_value=999999999)
            user_name = fake.user_name()
            name = fake.name()
            ssn = fake.ssn(min_age=18, max_age=50)
            password = hashlib.sha256(fake.password(length=10, special_chars=True, digits=True, upper_case=True, lower_case=True).encode())
            address = fake.address()
            province = fake.province()
            city = fake.city()
            phone = fake.phone_number()
            job = profile.get('job')
            sex = profile.get('sex')
            mail = profile.get('mail')
            birthday = profile.get('birthdate')
            current_time = int(time.time())

            sql_add = f"INSERT INTO user VALUES ({user_id}, '{user_name}', '{password}', '{name}', '{ssn}', '{address}', '{province}', '{city}'," \
                      f" '{phone}', '{job}', '{sex}', '{mail}', '{birthday}', 0, 0, {current_time})"
            con.sql(sql_add)

    return ResponseModel(code=0, msg="Success")


@app.get("/data/reset")
async def reset_data():
    with duckdb.connect(db_path) as con:
        try:
            con.sql("DROP TABLE user")
        except duckdb.duckdb.CatalogException:
            return ResponseModel(code=70000, msg="Table does not exist!")

    return ResponseModel(code=0, msg='Success')

