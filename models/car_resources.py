import json
import time
import datetime
from models.base_dataobject import BaseDO, DAOHelper

DEFAULT_BOOKING_PERIOD_HOURS = 2


class CarDO(BaseDO, metaclass=DAOHelper,
            indexes={"model_name": False, "reg_no": True},
            authorization={"manager"}):

    def __init__(self, model_name="N/A", launch_year="N/A", reg_no=None, **kwargs):
        super().__init__(**kwargs)
        self.model_name = model_name
        self.launch_year = launch_year
        self.reg_no = reg_no


class CarStateDO(BaseDO, metaclass=DAOHelper,
                 indexes={"reg_no": False},
                 relations={"reg_no": CarDO},
                 authorization={"customer"}):
    def __init__(self, reg_no="", booked_by="", booked_till="", **kwargs):
        super().__init__(**kwargs)
        self.reg_no = reg_no
        self.booked_by = booked_by or kwargs.get("last_updated_by", "")
        self.booked_till = CarStateDO.get_datetime_till_booked().strftime("%d/%m/%YT%H:%M:%S")

    @staticmethod
    def get_datetime_till_booked():
        dt = datetime.datetime.now()
        delta = datetime.timedelta(hours=DEFAULT_BOOKING_PERIOD_HOURS)
        return dt + delta

    def validate(self, obj=None):
        res, objects = CarStateDO.dao.get({"reg_no" : self.reg_no})

        if not res or not objects:
            return True, None

        current_datetime = datetime.datetime.now().strftime("%d/%m/%YT%H:%M:%S")
        for obj in objects:
            booked_till = json.loads(obj)["content"]["booked_till"]
            #print(booked_till)
            #print(current_datetime)
            if time.strptime(booked_till, "%d/%m/%YT%H:%M:%S") >= \
                time.strptime(current_datetime, "%d/%m/%YT%H:%M:%S"):
                return False, f'Car with reg_no:{self.reg_no} is already reserved till:{booked_till}'

        return True, None
