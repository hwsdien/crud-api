from pydantic import BaseModel
from typing import Union


class UserModel(BaseModel):
    user_id: Union[int, None] = None
    user_name: Union[str, None] = None
    password: Union[str, None] = None
    name: Union[str, None] = None
    ssn: Union[str, None] = None
    birthday: Union[str, None] = None
    address: Union[str, None] = None
    province: Union[str, None] = None
    city: Union[str, None] = None
    phone: Union[str, None] = None
    job: Union[str, None] = None
    sex: Union[str, None] = None
    mail: Union[str, None] = None
    is_active: Union[bool, None] = False
    is_delete: Union[bool, None] = False
    last_time: Union[int, None] = 0


class QueryModel(BaseModel):
    user_id: Union[int, None] = None
    user_name: Union[str, None] = None
    name: Union[str, None] = None
    ssn: Union[str, None] = None
    province: Union[str, None] = None
    city: Union[str, None] = None
    phone: Union[str, None] = None
    sex: Union[str, None] = None


class ResponseModel(BaseModel):
    code: int
    msg: str
    data: Union[object, None] = None

