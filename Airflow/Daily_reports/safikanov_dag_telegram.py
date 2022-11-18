from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pandahouse as ph
import telegram
import matplotlib.pyplot as plt
import seaborn as sns
import io


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

#конекшн

connection = {'host': 'hidden_host',
                      'database':'hidden_database',
                      'user':'hidden_user', 
                      'password':'hidden_password'
                     }
# размер графиков

sns.set(rc={'figure.figsize':(20,10)})

# Запрос 

query = '''
SELECT 
    toDate(time) as date,
    count(distinct user_id) as dau,
    countIf(user_id, action ='like') as likes,
    countIf(user_id, action ='view') as views,
    countIf(user_id, action ='like')/countIf(user_id, action ='view') as ctr
FROM 
    simulator_20220920.feed_actions 
WHERE 
    toDate(time) > yesterday() -7 and toDate(time) <=yesterday()
GROUP BY 
    date
'''

# Дефолтные параметры, которые прокидываются в таски

default_args = {
'owner': 'r-safikanov-11',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=1),
'start_date': datetime(2022, 10, 9)
}

# Интервал запуска DAG 11:00

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def safikanov_dag_telegram():
    
    @task()
    def make_report():
        def feed_daily_report(chat=None):
            chat_id = chat or 116500489 #my chat_id

            bot = telegram.Bot(token='hidden_bot_token')

            df = ph.read_clickhouse(query, connection=connection)
            msg = '''Key metrics for {}: 

                DAU = {},
                Likes = {},
                Views = {},
                CTR = {:.4f}'''.format(df.date[6].strftime("%d/%m/%Y")
                                       ,df.dau[6]
                                       ,df.likes[6]
                                       ,df.views[6]
                                       ,df.ctr[6])
            bot.sendMessage(chat_id=chat_id, text=msg)

            df['dt'] = df['date'].dt.strftime("%a %d/%m")

            plt.subplot (2, 2, 1)
            sns.lineplot(df.dt, df.dau)
            plt.xlabel(" ")
            plt.title ("DAU")

            plt.subplot (2, 2, 2)
            sns.lineplot(df.dt, df.ctr)
            plt.xlabel(" ")
            plt.title('CTR')

            plt.subplot (2, 2, 3)
            sns.lineplot(df.dt, df.likes)
            plt.xlabel(" ")
            plt.title('Likes')

            plt.subplot (2, 2, 4)
            sns.lineplot(df.dt, df.views)
            plt.xlabel(" ")
            plt.title('Views')

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'feed_daily.png'
            plt.close()

            bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        try:
            feed_daily_report()
        except Exception as e:
            print(e)

    make_report()

safikanov_dag_telegram = safikanov_dag_telegram()