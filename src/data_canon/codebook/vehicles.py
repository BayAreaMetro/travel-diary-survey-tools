"""Codebook enumerations for vehicle table."""

from data_canon.labeled_enum import LabeledEnum


class FuelType(LabeledEnum):
    """fuel_type value labels."""

    canonical_field_name = "fuel_type"

    GAS = (1, "Gas")
    HYBRID_HEV = (2, "Hybrid (HEV)")
    PLUG_IN_HYBRID_PHEV = (3, "Plug-in hybrid (PHEV)")
    ELECTRIC_EV = (4, "Electric (EV)")
    DIESEL = (5, "Diesel")
    FLEX_FUEL_FFV = (6, "Flex fuel (FFV)")
    OTHER_E_G_NATURAL_GAS_BIO_DIESEL = (997, "Other (e.g., natural gas, bio-diesel)")

class TollTransponder(LabeledEnum):
    """toll_transponder value labels."""

    canonical_field_name = "toll_transponder"

    NO = (0, "No")
    YES = (1, "Yes")

class VehicleNum(LabeledEnum):
    """vehicle_num value labels."""

    canonical_field_name = "vehicle_num"

    VALUE_1_VEHICLE = (1, "1 vehicle")
    VALUE_2_VEHICLES = (2, "2 vehicles")
    VALUE_3_VEHICLES = (3, "3 vehicles")
    VALUE_4_VEHICLES = (4, "4 vehicles")
    VALUE_5_VEHICLES = (5, "5 vehicles")
    VALUE_6_VEHICLES = (6, "6 vehicles")
    VALUE_7_VEHICLES = (7, "7 vehicles")
    VALUE_8_OR_MORE_VEHICLES = (8, "8 or more vehicles")

class Year(LabeledEnum):
    """year value labels."""

    canonical_field_name = "year"

    VALUE_1980_OR_EARLIER = (1980, "1980 or earlier")
    VALUE_1981 = (1981, "1981")
    VALUE_1982 = (1982, "1982")
    VALUE_1983 = (1983, "1983")
    VALUE_1984 = (1984, "1984")
    VALUE_1985 = (1985, "1985")
    VALUE_1986 = (1986, "1986")
    VALUE_1987 = (1987, "1987")
    VALUE_1988 = (1988, "1988")
    VALUE_1989 = (1989, "1989")
    VALUE_1990 = (1990, "1990")
    VALUE_1991 = (1991, "1991")
    VALUE_1992 = (1992, "1992")
    VALUE_1993 = (1993, "1993")
    VALUE_1994 = (1994, "1994")
    VALUE_1995 = (1995, "1995")
    VALUE_1996 = (1996, "1996")
    VALUE_1997 = (1997, "1997")
    VALUE_1998 = (1998, "1998")
    VALUE_1999 = (1999, "1999")
    VALUE_2000 = (2000, "2000")
    VALUE_2001 = (2001, "2001")
    VALUE_2002 = (2002, "2002")
    VALUE_2003 = (2003, "2003")
    VALUE_2004 = (2004, "2004")
    VALUE_2005 = (2005, "2005")
    VALUE_2006 = (2006, "2006")
    VALUE_2007 = (2007, "2007")
    VALUE_2008 = (2008, "2008")
    VALUE_2009 = (2009, "2009")
    VALUE_2010 = (2010, "2010")
    VALUE_2011 = (2011, "2011")
    VALUE_2012 = (2012, "2012")
    VALUE_2013 = (2013, "2013")
    VALUE_2014 = (2014, "2014")
    VALUE_2015 = (2015, "2015")
    VALUE_2016 = (2016, "2016")
    VALUE_2017 = (2017, "2017")
    VALUE_2018 = (2018, "2018")
    VALUE_2019 = (2019, "2019")
    VALUE_2020 = (2020, "2020")
    VALUE_2021 = (2021, "2021")
    VALUE_2022 = (2022, "2022")
    VALUE_2023 = (2023, "2023")
    VALUE_2024 = (2024, "2024")
