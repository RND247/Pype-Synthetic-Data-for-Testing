from abc import ABC
import string, random
from faker import Faker

class BaseColumn(ABC):
    python_type: type
    category: str
    category_func_dict = {
        "FirstName": Faker().first_name,
        "LastName": Faker().last_name,
        "FullName": Faker().name,
        "SSN": Faker().ssn,
        "FullAddress": Faker().address,
        "Country": Faker().country,
        "CountryCode": Faker().country_code,
        "City": Faker().city,
        "FullStreet": Faker().street_address,
        "StreetName": Faker().street_name,
        "PostCode": Faker().postcode,
        "PhoneNumber": Faker().phone_number,
        "Email": Faker().ascii_safe_email,
        "BBAN": Faker().bban,
        "IBAN": Faker().iban,
        "Date": Faker().date,
        "Time": Faker().time,
    }

    def __init__(self, python_type, category):
        self.python_type = python_type
        self.category = category

    def generate_value(self):
        pass


# Text Columns

class GeneralTextColumn(BaseColumn):
    max_length: int

    def __init__(self, category="GeneralText", max_length=20, is_capitalized=True, **kwargs):
        super().__init__(str, category)
        self.max_length = max_length
        self.is_capitalized = is_capitalized

    def generate_value(self):
        if self.category in self.category_func_dict.keys():
            value = self.category_func_dict[self.category]().lower()
            if self.max_length > 0:
                while len(value) > self.max_length:
                    value = self.category_func_dict[self.category]().lower()
            return value.capitalize() if self.is_capitalized else value
        value = ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, self.max_length)
                                     if self.max_length else random.randint(1, 50)))
        return value.capitalize() if self.is_capitalized else value


class FirstNameColumn(GeneralTextColumn):
    def __init__(self, max_length=15, is_capitalized=True, **kwargs):
        super().__init__("FirstName", max_length, is_capitalized, **kwargs)


class LastNameColumn(GeneralTextColumn):
    def __init__(self, max_length=15, is_capitalized=True, **kwargs):
        super().__init__("LastName", max_length, is_capitalized, **kwargs)


class FullNameColumn(GeneralTextColumn):
    def __init__(self, max_length=30, is_capitalized=True):
        super().__init__("FullName", max_length, is_capitalized)


class SocialSecurityNumber(GeneralTextColumn):
    def __init__(self):
        super().__init__("SSN", max_length=0)


class Address(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("FullAddress", max_length=max_length)


class Country(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("Country", max_length=max_length)


class CountryCode(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("CountryCode", max_length=max_length)


class City(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("City", max_length=max_length)


class FullStreet(GeneralTextColumn):
    def __init__(self, max_length=0, **kwargs):
        super().__init__("FullStreet", max_length=max_length, **kwargs)


class StreetName(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("StreetName", max_length=max_length)


class Postcode(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("PostCode", max_length=max_length)


class PhoneNumber(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("PhoneNumber", max_length=max_length)


class Email(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("Email", max_length=max_length)


class BBAN(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("BBAN", max_length=max_length)


class IBAN(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("IBAN", max_length=max_length)


class Date(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("Date", max_length=max_length)


class Time(GeneralTextColumn):
    def __init__(self, max_length=0):
        super().__init__("Time", max_length=max_length)


# Int Columns


class GeneralIntColumn(BaseColumn):
    min_value: int
    max_value: int

    def __init__(self, category="GeneralInt", min_value=0, max_value=100, **kwargs):
        super().__init__(int, category)
        self.min_value = min_value
        self.max_value = max_value

    def generate_value(self):
        value = random.randint(self.min_value, self.max_value)
        return value


class Age(GeneralIntColumn):
    def __init__(self, min_value=0, max_value=110, **kwargs):
        super().__init__("Age", min_value=min_value, max_value=max_value, **kwargs)


NAME_FUNC_DICT = {  # Wraparound. TODO: Fix it in generator.py - generate_by_category()
    "GeneralTextColumn": GeneralTextColumn,
    "FirstNameColumn": FirstNameColumn,
    "LastNameColumn": LastNameColumn,
    "FullNameColumn": FullNameColumn,
    "SocialSecurityNumber": SocialSecurityNumber,
    "Address": Address,
    "Country": Country,
    "CountryCode": CountryCode,
    "City": City,
    "FullStreet": FullStreet,
    "StreetName": StreetName,
    "Postcode": Postcode,
    "PhoneNumber": PhoneNumber,
    "Email": Email,
    "BBAN": BBAN,
    "IBAN": IBAN,
    "Date": Date,
    "Time": Time,
    "GeneralIntColumn": GeneralIntColumn,
    "Age": Age,
}
