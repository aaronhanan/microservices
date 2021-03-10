from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class FoodOrder(Base):
    """ Food Order """

    __tablename__ = "food_order"

    id = Column(Integer, primary_key=True)
    customer_id = Column(String(100), nullable=False)
    name = Column(String(100), nullable=False)
    phone = Column(String(50), nullable=False)
    order_date = Column(DateTime, nullable=False)

    def __init__(self, customer_id, name, phone):
        """ Initializes a food delivery order """
        self.customer_id = customer_id
        self.name = name
        self.phone = phone
        self.order_date = datetime.datetime.now()


    def to_dict(self):
        """ Dictionary Representation of a food delivery order """
        dict = {}
        dict['id'] = self.id
        dict['customer_id'] = self.customer_id
        dict['name'] = self.name
        dict['phone'] = self.phone
        dict['order_date'] = self.order_date

        return dict
