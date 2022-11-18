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

q = """select 
            ts,
            date, 
            hm,  
            users_feed,
            users_msg,
            likes,
            views,
            ctr,
            messages_sent
        from
        (SELECT
                toStartOfFifteenMinutes(time) as ts,
                toDate(ts) as date,
                formatDateTime(ts, '%R') as hm,
                count(distinct user_id) as users_feed,
                countIf(action == 'like') as likes,
                countIf(action == 'view') as views,
                (countIf(action == 'like') / countIf(action = 'view')) as ctr
        FROM simulator_20220920.feed_actions
        WHERE ts between  today() - 1 and toStartOfFifteenMinutes(now())
        GROUP BY ts, date
        ORDER BY ts) t1
        join         
        (SELECT
            toStartOfFifteenMinutes(time) as ts, 
            toDate(ts) as date,
            formatDateTime(ts, '%R') as hm,
            count(distinct user_id) as users_msg,
            count(reciever_id) as messages_sent
        FROM simulator_20220920.message_actions
        WHERE ts between  today() - 1 and toStartOfFifteenMinutes(now())
        GROUP BY ts, date
        ORDER BY ts) t2
        using ts
            """

# функция проверки на аномалии

def check_anomaly(df, metric, a=3, n = 5):
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['q75'] - df['q25']
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']

    df['up'] = df['up'].rolling(n, center=True, min_periods = 2).mean()
    df['low'] = df['low'].rolling(n, center=True, min_periods = 2).mean()


    if df[metric].iloc[-1] < df["low"].iloc[-1] or df[metric].iloc[-1] > df["up"].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
    return is_alert, df

# Дефолтные параметры, которые прокидываются в таски

default_args = {
'owner': 'r-safikanov-11',
'depends_on_past': False,
'retries': 2,
'retry_delay': timedelta(minutes=2),
'start_date': datetime(2022, 10, 15)
}

# Интервал запуска DAG 15 минут

schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def safikanov_alert():

    @task()
    def run_alerts():
        def alerting_system(chat=None):
            chat_id = chat or 116500489 #my chat_id
            
            bot = telegram.Bot(token='hidden_bot_token')
            
            df = ph.read_clickhouse(q, connection=connection)

            metrics_list = ['users_feed', 'users_msg', 'likes', 'views', 'ctr', 'messages_sent']

            for metric in metrics_list:
                df_copy = df[['ts', 'date', 'hm', metric]].copy()
                is_alert, df_copy = check_anomaly(df_copy, metric)

                diff = 1 - (df_copy[metric].iloc[-1] / df_copy[metric].iloc[-2])

                if is_alert == 1:

                    if diff < 0:
                        marker = '❎'
                    else:
                        marker = '❌'

                    msg = '''{}
                    @safikanooov
                    Metric *{}*:
                    Current value {:.2f} 
                    _Difference {:.2%}_'''.format(marker*3,
                                                  metric,
                                                  df_copy[metric].iloc[-1],
                                                  abs(diff))

                    ax = sns.lineplot(x = df_copy['hm'], y = df_copy[metric], label = metric)
                    ax = sns.lineplot(x = df_copy['hm'], y = df_copy['up'], label = 'up')
                    ax = sns.lineplot(x = df_copy['hm'], y = df_copy['low'], label = 'low')
                    
                    for ind, label in enumerate(ax.get_xticklabels()):
                        if ind % 2 == 0:
                            label.set_visible(True)
                        else:
                            label.set_visible(False)

                    ax.set(xlabel = 'Time')
                    ax.set_title('График метрики {} за последние сутки'.format(metric), fontsize=18)
                    ax.set(ylim=(0, None))
                    plt.xticks(rotation=45)

                    plot_object = io.BytesIO()
                    plt.savefig(plot_object)
                    plot_object.seek(0)
                    plot_object.name = '{}.png'.format('metric')
                    plt.close()

                    bot.sendMessage(chat_id=chat_id, text=msg, parse_mode="Markdown")
                    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

        try:
            alerting_system()
        except Exception as e:
            print(e)

    run_alerts()
    
safikanov_alert = safikanov_alert()
