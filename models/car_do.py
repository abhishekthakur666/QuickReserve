import datetime
from models.base_dataobject import BaseDO, DAOHelper


DEFAULT_BOOKING_PERIOD_HOURS = 2


class CarStateDO(BaseDO, metaclass=DAOHelper,
                 indexes={"car_reg_no": True},
                 authorization={"customer"}):
    def __init__(self, car_reg_no="", booked_by="", booked_till="", **kwargs):
        super().__init__(**kwargs)
        self.car_reg_no = car_reg_no
        self.booked_by = booked_by
        self.booked_till = booked_till

    @staticmethod
    def get_datetime_till_booked():
        dt = datetime.datetime.now()
        delta = datetime.timedelta(hours=DEFAULT_BOOKING_PERIOD_HOURS)
        return dt + delta


class CarDO(BaseDO, metaclass=DAOHelper,
            indexes={"model_name": False, "reg_no": True},
            authorization={"manager"}):

    def __init__(self, model_name="N/A", launch_year="N/A", reg_no=None, **kwargs):
        super().__init__(**kwargs)
        self.model_name = model_name
        self.launch_year = launch_year
        self.reg_no = reg_no
