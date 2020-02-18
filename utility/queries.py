import os
import psycopg2 as psy
import numpy as np
import pandas as pd
# import sqlalchemy import create_engine
from datetime import datetime
import matplotlib.pyplot as plt
import pickle


def run_all():
    # Parameters-------
    get_throttle = 0
    get_crunchtime = 0
    get_CSAT = 0
    get_Kustomer = 0
    get_mapping = 0 # for entity regions
    get_scorecard = 1
    # ------------------

    str1_1 = """ 
    with throttle as (
    
        SELECT
            *
            FROM prod_reports.throttle_throughput as THRO
            WHERE
              THRO.monday_start >= getdate()-90 /* Look back N days to */
              --AND
              --THRO.entity = '90'
    
        )
    
    SELECT * FROM throttle
    ORDER by entity,date,interval; -- sort by store, then date, then timeslot
    """

    str2 = """
    with labor as (
    
        SELECT
            CRUNCH.shift_datetime_in_adjusted as time_in,
            CRUNCH.shift_datetime_out_adjusted as time_out,
            CRUNCH.location_id as loc,
            CRUNCH.hours_adjusted as hours_adj
            FROM reporting.crunchtime_labor_actual_shift as CRUNCH
            WHERE
              time_in >= getdate()-90 /* Look back N days to */
              --AND
              --CRUNCH.location_id = '19'
    
    
        )
    
    --SELECT
    --    date_trunc('minute', shift_datetime_in_adjusted) - (CAST(EXTRACT(MINUTE FROM shift_datetime_in_adjusted) AS integer) % 15) * interval '1 minute' AS trunc_15_minute,
    --    count(*)
    --FROM labor
    --GROUP BY trunc_15_minute
    --ORDER BY trunc_15_minute;
    select * from labor
    ORDER by time_in;     
    """
    str3 = """
    with CSAT as (
        SELECT
          --PO.customer_id as customer_id,
          --PO.feedbackable_id as order_id
          PO.entity_id as entity_id, -- only really need store-level anyway, not so much order-level
          PO.order_timestamp_local::DATE as date,
          PO.response as reponse,
          PO.rating as ratings
        FROM
          prod_reports.csat_desktop_dashboard as PO
        WHERE
          --BO.description != 'TESTING DO NOT MAKE'
          --AND BO.description != 'testing do not make'
          --CHARINDEX('TESTING DO NOT MAKE', description) > 0
          --description LIKE '%testing do not make%'
          --AND description LIKE '%TESTING DO NOT MAKE%'
          --AND description LIKE '%OLO%'
          --description LIKE '%olo%'
          --(PO.order_type = 'pickup' OR PO.order_type = 'outpost' OR PO.order_type = 'delivery')
          --AND
          order_timestamp_local::DATE >= getdate()-360 /* Look back N days to average portions over */
        GROUP BY
          1,2,3,4
    )
    
    SELECT * FROM CSAT  
    """
    str4 = """
    with Kustomer as (
        SELECT
          --PO.customer_id as customer_id,
          --PO.feedbackable_id as order_id
          KU.entity_id as entity_id, -- only really need store-level anyway, not so much order-level
          KU.created_timestamp_local::DATE as date,
          KU.label as label
          --KU.rating as ratings
        FROM
          summary.cx_kustomer as KU
        WHERE
          --BO.description != 'TESTING DO NOT MAKE'
          --AND BO.description != 'testing do not make'
          --CHARINDEX('TESTING DO NOT MAKE', description) > 0
          --description LIKE '%testing do not make%'
          --AND description LIKE '%TESTING DO NOT MAKE%'
          --AND description LIKE '%OLO%'
          --description LIKE '%olo%'
          --(PO.order_type = 'pickup' OR PO.order_type = 'outpost' OR PO.order_type = 'delivery')
          --AND
          created_timestamp_local::DATE >= getdate()-360 /* Look back N days to average portions over */
        GROUP BY
          1,2,3
    )
    
    SELECT * FROM Kustomer    
    """

    str5 = """
    with mapping as (

        SELECT
            MAP.entity as entity,
            MAP.region as region
            FROM mapping.entities as MAP

        )

    --SELECT
    select * from mapping
    """

    str6 = """
    with score as (

        SELECT
            SC.entity as entity,
            SC.gross_sales_actual as gross_sales
            FROM prod_reports.scorecards as SC
            WHERE
              SC.year = '2019'
              --AND
              --CRUNCH.location_id = '19'


        )

    select * from score
    """
    with open("utility/config.txt", "r") as f:
        data = f.readlines()
    db, h, p, u, pw = [d.split('=')[1].split('\n')[0] for d in data]

    conn = psy.connect(dbname=db,
                       host=h,
                       port=p,
                       user=u,
                       password=pw)

    print('connected to cluster...')

    if (get_throttle == 1):
        cur = conn.cursor()
        cur.execute(str1_1)  # execute query on RedShift cluster
        print("query success!")
        data4 = np.array(cur.fetchall())
        df4 = pd.DataFrame(data4)
        df4.to_pickle('data/TT_Data_days.pkl')
        print("Data Saved..")
        # cur.close()
        # conn.close()

    if (get_crunchtime == 1):
        cur3 = conn.cursor()
        cur3.execute(str2)  # execute query on RedShift cluster
        print("query success!")
        data6 = np.array(cur3.fetchall())
        df6 = pd.DataFrame(data6)
        df6.to_pickle('data/CRUNCHTIME_Data_90days.pkl')
        print("Data Saved..")
        cur3.close()
        # conn.close()

    if (get_CSAT == 1):
        cur4 = conn.cursor()
        cur4.execute(str3)  # execute query on RedShift cluster
        print("query success!")
        data7 = np.array(cur4.fetchall())
        df7 = pd.DataFrame(data7)
        df7.to_pickle('data/CSAT_Data_90days.pkl')
        print("Data Saved..")
        cur4.close()
        # conn.close()

    if (get_Kustomer == 1):
        cur5 = conn.cursor()
        cur5.execute(str4)  # execute query on RedShift cluster
        print("query success!")
        data8 = np.array(cur5.fetchall())
        df8 = pd.DataFrame(data8)
        df8.to_pickle('data/Kustomer_Data_90days.pkl')
        print("Data Saved..")
        cur5.close()
        # conn.close()
    if (get_mapping == 1):
        cur6 = conn.cursor()
        cur6.execute(str5)  # execute query on RedShift cluster
        print("query success!")
        data9 = np.array(cur6.fetchall())
        df9 = pd.DataFrame(data9)
        df9.to_pickle('data/region_mapping_table.pkl')
        print("Data Saved..")
        cur6.close()
        # conn.close()
    if (get_scorecard == 1):
        cur7 = conn.cursor()
        cur7.execute(str6)  # execute query on RedShift cluster
        print("query success!")
        data10 = np.array(cur7.fetchall())
        df10 = pd.DataFrame(data10)
        df10.to_pickle('data/scorecard.pkl')
        print("Data Saved..")
        cur7.close()

    # cur.execute("SELECT t.* FROM reporting.gravy_loyalties t WHERE created_at BETWEEN '06/25/2018' and '07/25/2018'")  # loyalties table
    # data4 = np.array(cur.fetchall())
    # df4 = pd.DataFrame(data4)

    # cur.close()
    conn.close()

    list1 = []


class ML_model:
    pass

    if __name__ == "__queries__":
        run_all()