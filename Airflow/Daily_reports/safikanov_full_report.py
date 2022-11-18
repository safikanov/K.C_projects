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

q = """select * from 
    (SELECT 
        toDate(time) as date,
        count(distinct user_id) as dau,
        countIf(user_id, action ='like') as likes,
        countIf(user_id, action ='view') as views,
        countIf(user_id, action ='like')/countIf(user_id, action ='view') as ctr
    FROM 
        simulator_20220920.feed_actions
    WHERE 
        toDate(time) >= yesterday() -7 and toDate(time) <=yesterday()
    GROUP BY 
        date) f
    join 
        (select 
            toDate(time) as date,
            count(user_id) as messages,
            count(distinct(user_id)) dau_msg
        from 
            simulator_20220920.message_actions
            where toDate(time) >= yesterday() - 7 and toDate(time) <=yesterday()
        group by 
            date) m on m.date = f.date
            """

# Дефолтные параметры, которые прокидываются в таски

default_args = {
'owner': 'r-safikanov-11',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=2),
'start_date': datetime(2022, 10, 15)
}

# Интервал запуска DAG 11:00

schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def safikanov_full_report():
    
    @task()
    def make_report():
        def feed_daily_report(chat=None):
            chat_id = chat or 116500489 #my chat_id

            bot = telegram.Bot(token='hidden_bot_token')

            df = ph.read_clickhouse(q, connection=connection)
            msg = '''
            * Report date: {} *
            -----------------------------
            *DAU by features*:
            -----------------------------
            Feed yesterday: {} 
            _AVG last week_: {:.0f}
            *Difference*: {:.2%}
            .............................
            Messages yesterday: {} 
            _AVG last week_: {:.0f}
            *Difference*: {:.2%}
            -----------------------------
            *Key metrics*:
            -----------------------------
            *Likes* yesterday: {} 
            _AVG last week_: {:.0f}
            *Difference*: {:.2%}
            .............................
            *Views* yesterday: {} 
            _AVG last week_: {:.0f}
            *Difference*: {:.2%}
            .............................
            *CTR* yesterday: {:.4f} 
            _AVG last week_: {:.4f}
            *Difference*: {:.2%}
            .............................
            *Messages* sent yesterday: {} 
            _AVG last week_: {:.0f}
            *Difference*: {:.2%}
            -----------------------------'''.format(df.date[7].strftime("%d/%m/%Y")
                                                    ,df.dau[7]
                                                    ,df[:-1].dau.mean()
                                                    ,(df.dau[7]/df[:-1].dau.mean())-1
                                                    ,df.dau_msg[7]
                                                    ,df[:-1].dau_msg.mean()
                                                    ,(df.dau_msg[7]/df[:-1].dau_msg.mean())-1
                                                    ,df.likes[7]
                                                    ,df[:-1].likes.mean()
                                                    ,(df.likes[7]/df[:-1].likes.mean())-1
                                                    ,df.views[7]
                                                    ,df[:-1].views.mean()
                                                    ,(df.views[7]/df[:-1].views.mean())-1
                                                    ,df.ctr[7]
                                                    ,df[:-1].ctr.mean()
                                                    ,(df.ctr[7]/df[:-1].ctr.mean())-1
                                                    ,df.messages[7]
                                                    ,df[:-1].messages.mean()
                                                    ,(df.messages[7]/df[:-1].messages.mean())-1
                                                )
            bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="Markdown")

            df['dt'] = df['date'].dt.strftime("%a %d/%m")

            plt.subplot (2, 2, 1)
            sns.lineplot(df.dt, df.messages)
            plt.axhline(y=df[:-1].messages.mean(), color='grey', linestyle='--',label='Mean')
            plt.xlabel(" ")
            plt.title ("Messages")
            plt.legend() 

            plt.subplot (2, 2, 2)
            sns.lineplot(df.dt, df.ctr)
            plt.axhline(y=df[:-1].ctr.mean(), color='grey', linestyle='--',label='Mean')
            plt.xlabel(" ")
            plt.title('CTR')
            plt.legend() 

            plt.subplot (2, 2, 3)
            sns.lineplot(df.dt, df.likes)
            plt.axhline(y=df[:-1].likes.mean(), color='grey', linestyle='--',label='Mean')
            plt.xlabel(" ")
            plt.title('Likes')
            plt.legend() 

            plt.subplot (2, 2, 4)
            sns.lineplot(df.dt, df.views)
            plt.axhline(y=df[:-1].views.mean(), color='grey', linestyle='--',label='Mean')
            plt.xlabel(" ")
            plt.title('Views')
            plt.legend() 

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'feed_daily.png'
            plt.close()

            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
            
            file_object = io.StringIO()
            df.to_csv(file_object, index=True)
            file_object.seek(0)
            file_object.name = 'report_last_week.csv'

            bot.sendDocument(chat_id=chat_id, document = file_object)

        try:
            feed_daily_report()
        except Exception as e:
            print(e)

    make_report()

safikanov_full_report = safikanov_full_report()