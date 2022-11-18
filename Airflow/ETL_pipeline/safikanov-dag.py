from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#коннекшн

connection = {'host': 'hidden_host',
                      'database':'hidden_database',
                      'user':'hidden_user', 
                      'password':'hidden_password'
                     }

connection2 = {'host': 'hidden_host',
                      'database':'hidden_database',
                      'user':'hidden_user', 
                      'password':'hidden_password'
                     }
#запросы

q = """select 
            user_id,
            countIf(action='view') AS "views",
            countIf(action='like') AS "likes",
            gender,
            age,
            os
        from
            simulator_20220920.feed_actions
            where toDate(time) = today() - 1
        group by
            user_id,
            gender,
            age,
            os """

q2 = """select t3.*, m.gender,m.os,m.age from (
        select 
            distinct user_id,
            msg_received,
            msg_send,
            send_to,
            received_from,
            reciever_id
        from 
            (select 
                distinct user_id,
                count(reciever_id) as msg_send,
                count(distinct(reciever_id)) as send_to
            from 
                simulator_20220920.message_actions
                where toDate(time) = today() - 1
            group by 
                user_id) t1
            full join
            (select 
                distinct reciever_id, 
                count(user_id) as msg_received,
                count(distinct(user_id)) received_from
            from 
                simulator_20220920.message_actions
                where toDate(time) = today() - 1
            group by 
                reciever_id) t2 on t1.user_id = t2.reciever_id) t3
        join (select 
                    distinct user_id, 
                    age,
                    gender,
                    os
                from simulator_20220920.message_actions
                group by 
                    user_id,
                    age,
                    gender,
                    os) m on m.user_id = t3.user_id or m.user_id = t3.reciever_id"""

q3 = """
CREATE TABLE IF NOT EXISTS test.safikanov_dag (
    event_date Date,
    dimension String,
    dimension_value String,
    views UInt64,
    likes UInt64,
    messages_received UInt64,
    messages_sent UInt64,
    users_sent UInt64,
    users_received UInt64
)
ENGINE = MergeTree()
ORDER BY event_date
"""


# Дефолтные параметры, которые прокидываются в таски

default_args = {
'owner': 'r-safikanov-11',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=5),
'start_date': datetime(2022, 10, 9)
}

# Интервал запуска DAG 12:00

schedule_interval = '0 12 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def safikanov_dag():
    
    @task()
    def extract_feed():
        df_feed = ph.read_clickhouse(q, connection=connection)
        return df_feed

    @task()
    def extract_msg():
        df_msg = ph.read_clickhouse(q2, connection=connection)
        df_msg['user_id'] = np.where(df_msg['user_id'] == 0, df_msg['reciever_id'],df_msg['user_id'])
        return df_msg

    @task
    def merge_cubes(df_feed, df_msg):
        df = df_feed.merge(df_msg, how='outer', on=['user_id','os','gender','age'])
        df = df.fillna(0)
        for column in ['views','likes','msg_received','msg_send','send_to','received_from','reciever_id']:
            df[column] = df[column].astype(int)
        return df

    @task
    def transfrom_os(df):
        df_cube_os = df.groupby('os').sum().reset_index().rename(columns = {'os' : 'dimension_value'})
        df_cube_os.insert(0, 'dimension', 'os', False)
        df_cube_os.drop(columns = ['user_id','reciever_id','gender','age'],axis = 1, inplace=True)
        df_cube_os.rename(columns={'msg_received':'messages_received',
                                  'msg_send':'messages_sent',
                                  'send_to':'users_sent',
                                  'received_from':'users_received'}, inplace=True)
        return df_cube_os

    @task
    def transfrom_age(df):
        df_cube_age = df.groupby('age').sum().reset_index().rename(columns = {'age' : 'dimension_value'})
        df_cube_age.insert(0, 'dimension', 'age', False)
        df_cube_age.drop(columns = ['user_id','reciever_id','gender'],axis = 1, inplace=True)
        df_cube_age.rename(columns={'msg_received':'messages_received',
                                  'msg_send':'messages_sent',
                                  'send_to':'users_sent',
                                  'received_from':'users_received'}, inplace=True)
        return df_cube_age

    @task
    def transfrom_gender(df):
        df_cube_gender = df.groupby('gender').sum().reset_index().rename(columns = {'gender' : 'dimension_value'})
        df_cube_gender.insert(0, 'dimension', 'gender', False)
        df_cube_gender.drop(columns = ['user_id','reciever_id','age'],axis = 1, inplace=True)
        df_cube_gender.rename(columns={'msg_received':'messages_received',
                                  'msg_send':'messages_sent',
                                  'send_to':'users_sent',
                                  'received_from':'users_received'}, inplace=True)
        return df_cube_gender

    @task
    def union_cubes(df_cube_os, df_cube_age, df_cube_gender):
        final_cube = pd.concat([df_cube_os, df_cube_gender, df_cube_age],ignore_index = True, sort = False)
        final_cube.insert(0, 'event_date',(pd.to_datetime("today") - pd.Timedelta(1, unit='D')).strftime("%Y-%m-%d"), False)
        return final_cube

    @task
    def load_table(final_cube):
        ph.execute(query=q3, connection=connection2)
        ph.to_clickhouse(final_cube, 'safikanov_dag',connection=connection2, index=False)


    df_feed = extract_feed()
    df_msg = extract_msg()
    df = merge_cubes(df_feed, df_msg)
    df_cube_os = transfrom_os(df)
    df_cube_age = transfrom_age(df)
    df_cube_gender = transfrom_gender(df)
    final_cube = union_cubes(df_cube_os, df_cube_age, df_cube_gender)
    load_table(final_cube)

safikanov_dag = safikanov_dag()