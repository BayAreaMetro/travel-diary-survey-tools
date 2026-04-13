"""Synthetic toy data builders for E2E integration tests.

All functions return Polars DataFrames (or dicts for JSON) with correct dtypes.
No file I/O -- callers decide where to materialize the data.

All coordinates and data are fully synthetic -- no real PII.
"""

import math
import random
from datetime import date, datetime

import polars as pl

# ---------------------------------------------------------------------------
# Coordinates (synthetic SF Bay Area -- no real addresses)
# ---------------------------------------------------------------------------
COORDS = {
    "home_a": (37.7750, -122.4180),
    "home_b": (37.7620, -122.4350),
    "home_c": (37.7480, -122.4100),
    "home_d": (37.7830, -122.3950),
    "home_e": (37.7390, -122.4000),
    "home_f": (37.7910, -122.3870),
    "home_g": (37.7150, -122.4500),
    "home_h": (37.7500, -122.4420),
    "home_i": (37.7680, -122.3800),
    "home_j": (37.7250, -122.4700),
    "home_k": (37.7550, -122.4050),
    "home_l": (37.7350, -122.4250),
    "work_1": (37.7900, -122.3960),
    "work_2": (37.7850, -122.4010),
    "work_3": (37.7600, -122.3890),
    "school_1": (37.7700, -122.4200),
    "shop_1": (37.7650, -122.4260),
    "shop_2": (37.7710, -122.4150),
    "meal_1": (37.7680, -122.4300),
    "errand_1": (37.7720, -122.4130),
    "social_1": (37.7580, -122.4400),
    "bart_station": (37.7840, -122.4080),
    "bart_dest": (37.7950, -122.3930),
    "lunch_1": (37.7870, -122.3980),
}

DAY_DATE_1 = date(2024, 3, 11)  # Monday
DAY_DATE_2 = date(2024, 3, 12)  # Tuesday
DAY_DATE_SAT = date(2024, 3, 16)  # Saturday

# ---------------------------------------------------------------------------
# Canonical enum integer values (from data_canon.codebook)
# ---------------------------------------------------------------------------
FEMALE, MALE, GENDER_MISSING = 1, 2, 995
AGE_5_TO_15, AGE_18_TO_24, AGE_25_TO_34 = 2, 4, 5
AGE_35_TO_44, AGE_45_TO_54, AGE_55_TO_64 = 6, 7, 8
EMP_FULLTIME, EMP_PARTTIME, EMP_NOT_LOOKING = 1, 2, 5
STU_FULLTIME, STU_PARTTIME, STU_NONSTUDENT = 0, 1, 2
INC_UNDER_25K, INC_25_50K, INC_50_75K = 1, 2, 3
INC_75_100K, INC_100_200K, INC_200_PLUS, INC_MISSING = 4, 5, 6, 995
RES_SFH, RES_TOWNHOUSE, RES_MULTIFAMILY, RES_CONDO_5_50, RES_MISSING = 1, 2, 3, 4, 995
OWN, RENT, RENT_OWN_MISSING = 1, 2, 995
PC_HOME, PC_WORK, PC_SCHOOL = 1, 2, 4
PC_ESCORT, PC_SHOP, PC_MEAL = 6, 7, 8
PC_SOCIALREC, PC_ERRAND, PC_CHANGE_MODE = 9, 10, 11
PURP_HOME, PURP_WORK = 1, 2
MT_WALK, MT_BIKESHARE, MT_TNC, MT_CAR, MT_TRANSIT = 1, 3, 6, 8, 13
MODE_WALK, MODE_BIKE_RENTED, MODE_BART = 1, 4, 30
WP_FREE, WP_NOT_APPLICABLE, WP_MISSING = 1, 996, 995
YES, NO = 1, 0
DOW_MONDAY, DOW_TUESDAY, DOW_SATURDAY = 1, 2, 6
DRV_DRIVER, DRV_PASSENGER, DRV_MISSING = 1, 2, 995
JOB_FIXED = 1
SCH_ELEMENTARY, SCH_4YEAR = 5, 12
RACE_AFAM, RACE_ASIAN, RACE_WHITE, RACE_OTHER, RACE_PNTA = 1, 3, 5, 6, 999
ETH_NOT_HISPANIC, ETH_MEXICAN, ETH_MISSING = 1, 2, 995
CF_5_DAYS, CF_NEVER = 2, 996


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _meters(lat1, lon1, lat2, lon2):
    R = 6_371_000
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))


def _dt(d, hour, minute=0):
    return datetime(d.year, d.month, d.day, hour, minute, 0)


# ---------------------------------------------------------------------------
# Survey data builders  (return lists-of-dicts, converted to DataFrames below)
# ---------------------------------------------------------------------------


def _build_records():
    """Build all canonical records. Returns (hh, per, day, trip) as lists of dicts."""
    hh, per, day, trip = [], [], [], []
    _tc = [0]

    def _tid():
        _tc[0] += 1
        return _tc[0]

    def _trip(day_id, pid, hid, o, d, opc, dpc, mt, dep, arr, m1=None, ntrav=1, drv=None):
        tid = _tid()
        olat, olon = COORDS[o]
        dlat, dlon = COORDS[d]
        if drv is None:
            drv = DRV_DRIVER if mt == MT_CAR else DRV_PASSENGER if mt == MT_TNC else DRV_MISSING
        trip.append(
            {
                "unlinked_trip_id": tid,
                "day_id": day_id,
                "person_id": pid,
                "hh_id": hid,
                "linked_trip_id": tid,
                "o_lat": olat,
                "o_lon": olon,
                "d_lat": dlat,
                "d_lon": dlon,
                "o_purpose": PURP_HOME,
                "d_purpose": PURP_WORK,
                "o_purpose_category": opc,
                "d_purpose_category": dpc,
                "mode_type": mt,
                "mode_1": m1 if m1 is not None else mt,
                "mode_2": None,
                "mode_3": None,
                "mode_4": None,
                "duration_minutes": round((arr - dep).total_seconds() / 60, 1),
                "distance_meters": round(_meters(olat, olon, dlat, dlon), 1),
                "depart_time": dep,
                "arrive_time": arr,
                "travel_dow": dep.weekday() + 1,  # 1=Mon … 7=Sun
                "driver": drv,
                "num_travelers": ntrav,
                "complete": True,
                "unlinked_trip_weight": None,
            }
        )

    def _per(
        pid,
        hid,
        pn,
        age,
        gen,
        emp,
        stu,
        *,
        wloc=None,
        sloc=None,
        jt=None,
        st=None,
        wp=WP_NOT_APPLICABLE,
        wm=None,
        race=RACE_WHITE,
        eth=ETH_NOT_HISPANIC,
        cf=None,
        tp=NO,
        proxy=NO,
        ndays=1,
        comp=True,
    ):
        per.append(
            {
                "person_id": pid,
                "hh_id": hid,
                "person_num": pn,
                "age": age,
                "gender": gen,
                "employment": emp,
                "student": stu,
                "work_lat": COORDS[wloc][0] if wloc else None,
                "work_lon": COORDS[wloc][1] if wloc else None,
                "school_lat": COORDS[sloc][0] if sloc else None,
                "school_lon": COORDS[sloc][1] if sloc else None,
                "job_type": jt,
                "school_type": st,
                "work_park": wp,
                "work_mode": wm,
                "race": race,
                "ethnicity": eth,
                "telework_freq": None,
                "commute_freq": cf,
                "commute_subsidy_use_3": NO,
                "commute_subsidy_use_4": NO,
                "transit_pass": tp,
                "is_proxy": proxy,
                "num_days_complete": ndays,
                "complete": comp,
                "person_weight": None,
            }
        )

    def _day(did, pid, hid, dd, dow, pnum=1, dnum=1, comp=True):
        day.append(
            {
                "day_id": did,
                "person_id": pid,
                "hh_id": hid,
                "travel_date": datetime(dd.year, dd.month, dd.day),
                "travel_dow": dow,
                "person_num": pnum,
                "day_num": dnum,
                "complete": comp,
                "day_weight": None,
            }
        )

    # HH 1 - simple car commuter
    hh.append(
        {
            "hh_id": 1,
            "home_lat": COORDS["home_a"][0],
            "home_lon": COORDS["home_a"][1],
            "residence_rent_own": OWN,
            "residence_type": RES_SFH,
            "income_bin": INC_75_100K,
            "num_vehicles": 1,
            "num_people": 1,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        101,
        1,
        1,
        AGE_35_TO_44,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
    )
    _day(10101, 101, 1, DAY_DATE_1, DOW_MONDAY)
    _trip(
        10101,
        101,
        1,
        "home_a",
        "work_1",
        PC_HOME,
        PC_WORK,
        MT_CAR,
        _dt(DAY_DATE_1, 8),
        _dt(DAY_DATE_1, 8, 30),
    )
    _trip(
        10101,
        101,
        1,
        "work_1",
        "home_a",
        PC_WORK,
        PC_HOME,
        MT_CAR,
        _dt(DAY_DATE_1, 17),
        _dt(DAY_DATE_1, 17, 30),
    )

    # HH 2 - transit with mode change (4 unlinked -> 2 linked)
    hh.append(
        {
            "hh_id": 2,
            "home_lat": COORDS["home_b"][0],
            "home_lon": COORDS["home_b"][1],
            "residence_rent_own": RENT,
            "residence_type": RES_CONDO_5_50,
            "income_bin": INC_100_200K,
            "num_vehicles": 0,
            "num_people": 1,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        201,
        2,
        1,
        AGE_25_TO_34,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="bart_dest",
        jt=JOB_FIXED,
        wp=WP_NOT_APPLICABLE,
        wm=MT_TRANSIT,
        race=RACE_ASIAN,
        cf=CF_5_DAYS,
        tp=YES,
    )
    _day(20101, 201, 2, DAY_DATE_1, DOW_MONDAY)
    _trip(
        20101,
        201,
        2,
        "home_b",
        "bart_station",
        PC_HOME,
        PC_CHANGE_MODE,
        MT_WALK,
        _dt(DAY_DATE_1, 7, 30),
        _dt(DAY_DATE_1, 7, 45),
        m1=MODE_WALK,
    )
    _trip(
        20101,
        201,
        2,
        "bart_station",
        "bart_dest",
        PC_CHANGE_MODE,
        PC_WORK,
        MT_TRANSIT,
        _dt(DAY_DATE_1, 7, 45),
        _dt(DAY_DATE_1, 8, 15),
        m1=MODE_BART,
    )
    _trip(
        20101,
        201,
        2,
        "bart_dest",
        "bart_station",
        PC_WORK,
        PC_CHANGE_MODE,
        MT_TRANSIT,
        _dt(DAY_DATE_1, 17),
        _dt(DAY_DATE_1, 17, 30),
        m1=MODE_BART,
    )
    _trip(
        20101,
        201,
        2,
        "bart_station",
        "home_b",
        PC_CHANGE_MODE,
        PC_HOME,
        MT_WALK,
        _dt(DAY_DATE_1, 17, 30),
        _dt(DAY_DATE_1, 17, 45),
        m1=MODE_WALK,
    )

    # HH 3 - joint trip household (2 persons)
    hh.append(
        {
            "hh_id": 3,
            "home_lat": COORDS["home_c"][0],
            "home_lon": COORDS["home_c"][1],
            "residence_rent_own": OWN,
            "residence_type": RES_SFH,
            "income_bin": INC_100_200K,
            "num_vehicles": 2,
            "num_people": 2,
            "num_workers": 2,
            "complete": True,
            "hh_weight": None,
        }
    )
    for pn, pid, gen in [(1, 301, MALE), (2, 302, FEMALE)]:
        _per(
            pid,
            3,
            pn,
            AGE_35_TO_44,
            gen,
            EMP_FULLTIME,
            STU_NONSTUDENT,
            wloc="work_2",
            jt=JOB_FIXED,
            wp=WP_FREE,
            wm=MT_CAR,
            cf=CF_5_DAYS,
        )
        did = pid * 100 + 1
        _day(did, pid, 3, DAY_DATE_1, DOW_MONDAY, pnum=pn)
        _trip(
            did,
            pid,
            3,
            "home_c",
            "work_2",
            PC_HOME,
            PC_WORK,
            MT_CAR,
            _dt(DAY_DATE_1, 8),
            _dt(DAY_DATE_1, 8, 25),
            ntrav=2,
        )
        _trip(
            did,
            pid,
            3,
            "work_2",
            "home_c",
            PC_WORK,
            PC_HOME,
            MT_CAR,
            _dt(DAY_DATE_1, 17, 30),
            _dt(DAY_DATE_1, 17, 55),
            ntrav=2,
        )

    # HH 4 - multi-stop errands
    hh.append(
        {
            "hh_id": 4,
            "home_lat": COORDS["home_d"][0],
            "home_lon": COORDS["home_d"][1],
            "residence_rent_own": RENT,
            "residence_type": RES_TOWNHOUSE,
            "income_bin": INC_50_75K,
            "num_vehicles": 1,
            "num_people": 1,
            "num_workers": 0,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        401,
        4,
        1,
        AGE_45_TO_54,
        MALE,
        EMP_NOT_LOOKING,
        STU_NONSTUDENT,
        race=RACE_OTHER,
        eth=ETH_MEXICAN,
        cf=CF_NEVER,
    )
    _day(40101, 401, 4, DAY_DATE_1, DOW_MONDAY)
    for o, d, op, dp, dh, dm, ah, am in [
        ("home_d", "shop_1", PC_HOME, PC_SHOP, 9, 0, 9, 15),
        ("shop_1", "meal_1", PC_SHOP, PC_MEAL, 9, 45, 10, 0),
        ("meal_1", "errand_1", PC_MEAL, PC_ERRAND, 10, 45, 11, 0),
        ("errand_1", "shop_2", PC_ERRAND, PC_SHOP, 11, 30, 11, 45),
        ("shop_2", "home_d", PC_SHOP, PC_HOME, 12, 15, 12, 30),
    ]:
        _trip(40101, 401, 4, o, d, op, dp, MT_CAR, _dt(DAY_DATE_1, dh, dm), _dt(DAY_DATE_1, ah, am))

    # HH 5 - escort + school (parent + child)
    hh.append(
        {
            "hh_id": 5,
            "home_lat": COORDS["home_e"][0],
            "home_lon": COORDS["home_e"][1],
            "residence_rent_own": OWN,
            "residence_type": RES_SFH,
            "income_bin": INC_25_50K,
            "num_vehicles": 1,
            "num_people": 2,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        501,
        5,
        1,
        AGE_35_TO_44,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_3",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        race=RACE_AFAM,
        cf=CF_5_DAYS,
    )
    _per(
        502,
        5,
        2,
        AGE_5_TO_15,
        FEMALE,
        EMP_NOT_LOOKING,
        STU_FULLTIME,
        sloc="school_1",
        st=SCH_ELEMENTARY,
        race=RACE_AFAM,
        proxy=YES,
    )
    for pn, pid in [(1, 501), (2, 502)]:
        _day(pid * 100 + 1, pid, 5, DAY_DATE_1, DOW_MONDAY, pnum=pn)
    _trip(
        50101,
        501,
        5,
        "home_e",
        "school_1",
        PC_HOME,
        PC_ESCORT,
        MT_CAR,
        _dt(DAY_DATE_1, 7, 30),
        _dt(DAY_DATE_1, 7, 45),
    )
    _trip(
        50101,
        501,
        5,
        "school_1",
        "work_3",
        PC_ESCORT,
        PC_WORK,
        MT_CAR,
        _dt(DAY_DATE_1, 7, 45),
        _dt(DAY_DATE_1, 8, 10),
    )
    _trip(
        50101,
        501,
        5,
        "work_3",
        "home_e",
        PC_WORK,
        PC_HOME,
        MT_CAR,
        _dt(DAY_DATE_1, 17),
        _dt(DAY_DATE_1, 17, 25),
    )
    _trip(
        50201,
        502,
        5,
        "home_e",
        "school_1",
        PC_HOME,
        PC_SCHOOL,
        MT_CAR,
        _dt(DAY_DATE_1, 7, 30),
        _dt(DAY_DATE_1, 7, 45),
        drv=DRV_PASSENGER,
    )
    _trip(
        50201,
        502,
        5,
        "school_1",
        "home_e",
        PC_SCHOOL,
        PC_HOME,
        MT_CAR,
        _dt(DAY_DATE_1, 15),
        _dt(DAY_DATE_1, 15, 15),
        drv=DRV_PASSENGER,
    )

    # HH 6 - work subtour (lunch)
    hh.append(
        {
            "hh_id": 6,
            "home_lat": COORDS["home_f"][0],
            "home_lon": COORDS["home_f"][1],
            "residence_rent_own": RENT,
            "residence_type": RES_CONDO_5_50,
            "income_bin": INC_200_PLUS,
            "num_vehicles": 0,
            "num_people": 1,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        601,
        6,
        1,
        AGE_25_TO_34,
        FEMALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_NOT_APPLICABLE,
        wm=MT_WALK,
        cf=CF_5_DAYS,
    )
    _day(60101, 601, 6, DAY_DATE_1, DOW_MONDAY)
    _trip(
        60101,
        601,
        6,
        "home_f",
        "work_1",
        PC_HOME,
        PC_WORK,
        MT_WALK,
        _dt(DAY_DATE_1, 8),
        _dt(DAY_DATE_1, 8, 20),
    )
    _trip(
        60101,
        601,
        6,
        "work_1",
        "lunch_1",
        PC_WORK,
        PC_MEAL,
        MT_WALK,
        _dt(DAY_DATE_1, 12),
        _dt(DAY_DATE_1, 12, 10),
    )
    _trip(
        60101,
        601,
        6,
        "lunch_1",
        "work_1",
        PC_MEAL,
        PC_WORK,
        MT_WALK,
        _dt(DAY_DATE_1, 12, 45),
        _dt(DAY_DATE_1, 12, 55),
    )
    _trip(
        60101,
        601,
        6,
        "work_1",
        "home_f",
        PC_WORK,
        PC_HOME,
        MT_WALK,
        _dt(DAY_DATE_1, 17, 30),
        _dt(DAY_DATE_1, 17, 50),
    )

    # HH 7 - single-trip tour (didn't return home)
    hh.append(
        {
            "hh_id": 7,
            "home_lat": COORDS["home_g"][0],
            "home_lon": COORDS["home_g"][1],
            "residence_rent_own": RENT,
            "residence_type": RES_MULTIFAMILY,
            "income_bin": INC_UNDER_25K,
            "num_vehicles": 1,
            "num_people": 1,
            "num_workers": 0,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(701, 7, 1, AGE_18_TO_24, MALE, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
    _day(70101, 701, 7, DAY_DATE_1, DOW_MONDAY)
    _trip(
        70101,
        701,
        7,
        "home_g",
        "social_1",
        PC_HOME,
        PC_SOCIALREC,
        MT_CAR,
        _dt(DAY_DATE_1, 19),
        _dt(DAY_DATE_1, 19, 20),
    )

    # HH 8 - weekend recreation (2 retirees, potential joint trips)
    hh.append(
        {
            "hh_id": 8,
            "home_lat": COORDS["home_h"][0],
            "home_lon": COORDS["home_h"][1],
            "residence_rent_own": OWN,
            "residence_type": RES_SFH,
            "income_bin": INC_200_PLUS,
            "num_vehicles": 2,
            "num_people": 2,
            "num_workers": 0,
            "complete": True,
            "hh_weight": None,
        }
    )
    for pn, pid, gen, age in [(1, 801, MALE, AGE_55_TO_64), (2, 802, FEMALE, AGE_45_TO_54)]:
        _per(pid, 8, pn, age, gen, EMP_NOT_LOOKING, STU_NONSTUDENT, cf=CF_NEVER)
        did = pid * 100 + 1
        _day(did, pid, 8, DAY_DATE_SAT, DOW_SATURDAY, pnum=pn)
        _trip(
            did,
            pid,
            8,
            "home_h",
            "shop_1",
            PC_HOME,
            PC_SHOP,
            MT_CAR,
            _dt(DAY_DATE_SAT, 10),
            _dt(DAY_DATE_SAT, 10, 15),
        )
        _trip(
            did,
            pid,
            8,
            "shop_1",
            "meal_1",
            PC_SHOP,
            PC_MEAL,
            MT_CAR,
            _dt(DAY_DATE_SAT, 11),
            _dt(DAY_DATE_SAT, 11, 10),
        )
        _trip(
            did,
            pid,
            8,
            "meal_1",
            "home_h",
            PC_MEAL,
            PC_HOME,
            MT_CAR,
            _dt(DAY_DATE_SAT, 12, 30),
            _dt(DAY_DATE_SAT, 12, 45),
        )

    # HH 9 - TNC user
    hh.append(
        {
            "hh_id": 9,
            "home_lat": COORDS["home_i"][0],
            "home_lon": COORDS["home_i"][1],
            "residence_rent_own": RENT,
            "residence_type": RES_CONDO_5_50,
            "income_bin": INC_100_200K,
            "num_vehicles": 0,
            "num_people": 1,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        901,
        9,
        1,
        AGE_25_TO_34,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_2",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_TNC,
        cf=CF_5_DAYS,
    )
    _day(90101, 901, 9, DAY_DATE_1, DOW_MONDAY)
    _trip(
        90101,
        901,
        9,
        "home_i",
        "work_2",
        PC_HOME,
        PC_WORK,
        MT_TNC,
        _dt(DAY_DATE_1, 8, 30),
        _dt(DAY_DATE_1, 8, 50),
    )
    _trip(
        90101,
        901,
        9,
        "work_2",
        "home_i",
        PC_WORK,
        PC_HOME,
        MT_TNC,
        _dt(DAY_DATE_1, 18),
        _dt(DAY_DATE_1, 18, 20),
    )

    # HH 10 - bikeshare + part-time student
    hh.append(
        {
            "hh_id": 10,
            "home_lat": COORDS["home_j"][0],
            "home_lon": COORDS["home_j"][1],
            "residence_rent_own": RENT,
            "residence_type": RES_TOWNHOUSE,
            "income_bin": INC_50_75K,
            "num_vehicles": 0,
            "num_people": 1,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        1001,
        10,
        1,
        AGE_18_TO_24,
        FEMALE,
        EMP_PARTTIME,
        STU_PARTTIME,
        wloc="work_3",
        sloc="school_1",
        jt=JOB_FIXED,
        st=SCH_4YEAR,
        wp=WP_FREE,
        wm=MT_BIKESHARE,
        race=RACE_OTHER,
        cf=CF_5_DAYS,
        tp=YES,
    )
    _day(100101, 1001, 10, DAY_DATE_1, DOW_MONDAY)
    _trip(
        100101,
        1001,
        10,
        "home_j",
        "work_3",
        PC_HOME,
        PC_WORK,
        MT_BIKESHARE,
        _dt(DAY_DATE_1, 9),
        _dt(DAY_DATE_1, 9, 25),
        m1=MODE_BIKE_RENTED,
    )
    _trip(
        100101,
        1001,
        10,
        "work_3",
        "home_j",
        PC_WORK,
        PC_HOME,
        MT_BIKESHARE,
        _dt(DAY_DATE_1, 16),
        _dt(DAY_DATE_1, 16, 25),
        m1=MODE_BIKE_RENTED,
    )

    # HH 11 - multi-day traveler (2 travel days)
    hh.append(
        {
            "hh_id": 11,
            "home_lat": COORDS["home_k"][0],
            "home_lon": COORDS["home_k"][1],
            "residence_rent_own": OWN,
            "residence_type": RES_SFH,
            "income_bin": INC_100_200K,
            "num_vehicles": 1,
            "num_people": 1,
            "num_workers": 1,
            "complete": True,
            "hh_weight": None,
        }
    )
    _per(
        1101,
        11,
        1,
        AGE_45_TO_54,
        MALE,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_1",
        jt=JOB_FIXED,
        wp=WP_FREE,
        wm=MT_CAR,
        cf=CF_5_DAYS,
        ndays=2,
    )
    for dn, dd, dow in [(1, DAY_DATE_1, DOW_MONDAY), (2, DAY_DATE_2, DOW_TUESDAY)]:
        _day(110100 + dn, 1101, 11, dd, dow, dnum=dn)
    _trip(
        110101,
        1101,
        11,
        "home_k",
        "work_1",
        PC_HOME,
        PC_WORK,
        MT_CAR,
        _dt(DAY_DATE_1, 8),
        _dt(DAY_DATE_1, 8, 20),
    )
    _trip(
        110101,
        1101,
        11,
        "work_1",
        "home_k",
        PC_WORK,
        PC_HOME,
        MT_CAR,
        _dt(DAY_DATE_1, 17),
        _dt(DAY_DATE_1, 17, 20),
    )
    _trip(
        110102,
        1101,
        11,
        "home_k",
        "work_1",
        PC_HOME,
        PC_WORK,
        MT_CAR,
        _dt(DAY_DATE_2, 8, 15),
        _dt(DAY_DATE_2, 8, 35),
    )
    _trip(
        110102,
        1101,
        11,
        "work_1",
        "home_k",
        PC_WORK,
        PC_HOME,
        MT_CAR,
        _dt(DAY_DATE_2, 17, 30),
        _dt(DAY_DATE_2, 17, 50),
    )

    # HH 12 - incomplete household (missing data)
    hh.append(
        {
            "hh_id": 12,
            "home_lat": COORDS["home_l"][0],
            "home_lon": COORDS["home_l"][1],
            "residence_rent_own": RENT_OWN_MISSING,
            "residence_type": RES_MISSING,
            "income_bin": INC_MISSING,
            "num_vehicles": 1,
            "num_people": 1,
            "num_workers": 1,
            "complete": False,
            "hh_weight": None,
        }
    )
    _per(
        1201,
        12,
        1,
        AGE_55_TO_64,
        GENDER_MISSING,
        EMP_FULLTIME,
        STU_NONSTUDENT,
        wloc="work_2",
        jt=JOB_FIXED,
        wp=WP_MISSING,
        wm=MT_CAR,
        race=RACE_PNTA,
        eth=ETH_MISSING,
        cf=CF_5_DAYS,
        comp=False,
    )
    _day(120101, 1201, 12, DAY_DATE_1, DOW_MONDAY, comp=False)
    _trip(
        120101,
        1201,
        12,
        "home_l",
        "work_2",
        PC_HOME,
        PC_WORK,
        MT_CAR,
        _dt(DAY_DATE_1, 8),
        _dt(DAY_DATE_1, 8, 25),
    )
    _trip(
        120101,
        1201,
        12,
        "work_2",
        "home_l",
        PC_WORK,
        PC_HOME,
        MT_CAR,
        _dt(DAY_DATE_1, 17),
        _dt(DAY_DATE_1, 17, 25),
    )

    return hh, per, day, trip


def build_survey_dataframes():
    """Return canonical survey data as a dict of Polars DataFrames.

    Keys: households, persons, days, unlinked_trips
    """
    hh, per, day, trip = _build_records()
    return {
        "households": pl.DataFrame(hh),
        "persons": pl.DataFrame(per),
        "days": pl.DataFrame(day),
        "unlinked_trips": pl.DataFrame(trip),
    }


# ---------------------------------------------------------------------------
# PUMS
# ---------------------------------------------------------------------------


def build_pums_dataframes(n_hh=500, seed=42):
    """Return synthetic PUMS data as (hh_df, per_df) Polars DataFrames."""
    rng = random.Random(seed)
    pums_hh, pums_per = [], []
    sporder = {}

    for i in range(1, n_hh + 1):
        sn = f"2024HU{i:07d}"
        np_sz = rng.choices([1, 2, 3, 4, 5], weights=[30, 30, 20, 15, 5])[0]
        pums_hh.append(
            {
                "SERIALNO": sn,
                "PUMA": "00101",
                "STATE": "06",
                "WGTP": rng.randint(50, 400),
                "TYPEHUGQ": 1,
                "NP": np_sz,
                "HINCP": rng.choice([-5000, 15000, 35000, 60000, 85000, 120000, 250000]),
                "VEH": rng.choices([0, 1, 2, 3], weights=[15, 40, 35, 10])[0],
            }
        )
        sporder[sn] = 0
        for _ in range(np_sz):
            sporder[sn] += 1
            agep = rng.choices(
                [3, 10, 17, 22, 30, 40, 50, 58, 70, 80, 90],
                weights=[5, 8, 5, 10, 15, 15, 12, 10, 8, 7, 5],
            )[0]
            esr = 6 if agep < 16 else rng.choices([1, 3, 6], weights=[55, 5, 40])[0]
            pums_per.append(
                {
                    "SERIALNO": sn,
                    "SPORDER": sporder[sn],
                    "PUMA": "00101",
                    "STATE": "06",
                    "PWGTP": rng.randint(50, 400),
                    "SEX": rng.choice([1, 2]),
                    "ESR": esr,
                    "WKHP": rng.randint(35, 50) if esr == 1 else 0,
                    "AGEP": agep,
                    "SCHL": (
                        0
                        if agep < 5
                        else rng.randint(1, 12)
                        if agep < 18
                        else rng.choices(
                            [16, 18, 20, 21, 22, 24], weights=[10, 15, 25, 25, 15, 10]
                        )[0]
                    ),
                    "RAC1P": rng.choices([1, 2, 3, 5, 6, 8, 9], weights=[60, 13, 2, 6, 1, 10, 8])[
                        0
                    ],
                    "HISP": rng.choices([1, 2, 3, 4, 24], weights=[75, 10, 5, 2, 8])[0],
                }
            )

    return pl.DataFrame(pums_hh), pl.DataFrame(pums_per)


# ---------------------------------------------------------------------------
# Zone GeoJSON
# ---------------------------------------------------------------------------

ZONE_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"fipco": "001", "TAZ_NODE": 1, "MAZ_NODE": 1},
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [-122.55, 37.70],
                        [-122.55, 37.82],
                        [-122.35, 37.82],
                        [-122.35, 37.70],
                        [-122.55, 37.70],
                    ]
                ],
            },
        }
    ],
}
