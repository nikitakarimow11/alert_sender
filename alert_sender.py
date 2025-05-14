import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from datetime import datetime, timedelta
import pandahouse as ph
import io
import requests

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'HOST',
    'password': 'PASSWORD',
    'user': 'USER',
    'database': 'DATABASE'
}

default_args = {
    'owner': 'n-karimov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 4, 1)
}

schedule_interval = '*/15 * * * *'

my_token = '7217036445:AAFfAfpraRsj3Iu2Nf4tyeArSjysIJFTTMU'
bot = telegram.Bot(token=my_token)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def check_anomaly_karimov():

    def check_anomaly(df,metric,a=3,n=5):
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a*df['iqr']
        df['low'] = df['q25'] - a*df['iqr']

        df['up'] = df['up'].rolling(n, center = True).mean()
        df['low'] = df['low'].rolling(n, center = True).mean()


        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df

    @task()
    def run_alerts(chat=None):
        chat_id = chat or 1355514400

        q = """
                select toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    formatDateTime(ts, '%R') as hm,
                    count(distinct user_id) users_feed,
                    countIf(action == 'like') likes,
                    countIf(action == 'view') views
                from simulator_20250220.feed_actions
                where time > today()-1 and time < toStartOfFifteenMinutes(now())
                group by ts, date, hm
                order by ts
                """
        data = ph.read_clickhouse(query=q, connection=connection)

        metrics_list = ['users_feed', 'views', 'likes']
        for metric in metrics_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)

            if is_alert == 1 or True:
                msg = '''метрика {metric}:\n текущее значение {current_val:.2f}\nотклонение от предыдущего значения {last_val_diff:.2f}'''.format(metric = metric, current_val = df[metric].iloc[-1], last_val_diff = 1 - (df[metric].iloc[-1]/df[metric].iloc[-2]))

                sns.set(rc ={'figure.figsize':(16,10)})
                plt.tight_layout()

                ax = sns.lineplot(x=df['ts'], y = df[metric], label = 'metric')
                ax = sns.lineplot(x=df['ts'], y = df['up'], label = 'up')
                ax = sns.lineplot(x=df['ts'], y = df['low'], label = 'low')

                for ind, label in enumerate(ax.get_xticklabels()):
                    if ind % 15 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)

                ax.set(xlabel='time')
                ax.set(ylabel=metric)

                ax.set_title(metric)
                ax.set(ylim=(0, None))

                plot_object = io.BytesIO()
                plt.savefig(plot_object, format='png', dpi=300)
                plot_object.seek(0)
                plt.close()

                bot.sendMessage(chat_id=chat_id, text=msg)
                bot.send_photo(chat_id=chat_id, photo=plot_object)  

    run_alerts()
    
check_anomaly_karimov = check_anomaly_karimov()