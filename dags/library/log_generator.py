import random
import uuid
import csv
from faker import Faker
import os
from pathlib import Path
faker = Faker()
CHANNELS = ["Naver","Facebook","Youtube","Instagram","Google","Organic"]
USER_SIZE = 1000
AVG_SESSION_SIZE = 10
MONTH_RANGE = 7
channels_path = "/usr/local/airflow/tmp/channels.csv"
usc_path = "/usr/local/airflow/tmp/user_session_channels.csv"
session_timestamps_path = "/usr/local/airflow/tmp/session_timestamps.csv"
session_transactions_path = "/usr/local/airflow/tmp/session_transactions.csv"

class LogGenerator:
    def execute(self):
        self.write_channels()
        self.write_csv_session_with_user()
        self.write_session_transactions()
        return {
            'channels.csv': channels_path,
            'user_session_channels.csv': usc_path,
            'session_timestamps.csv': session_transactions_path,
            'session_transactions.csv'  : session_transactions_path
        }

    def write_channels(self):
        with open(channels_path, "w") as f:
            for ch in CHANNELS :
                f.write(ch+"\n")

    def write_csv_session_with_user(self):
        user_ids = list(range(1,USER_SIZE))
        divide_num = USER_SIZE // MONTH_RANGE
        print("DIVIDE NUM : ",divide_num)
        usc_f = open(usc_path, 'w')
        st_f = open(session_timestamps_path, 'w')
        usc_f.write('sessionid,userid,channel\n')
        st_f.write('sessionid,timestamp\n')
        # user session channels, session timestamps 생성
        for idx in range(MONTH_RANGE):
            random.shuffle(user_ids)
            users = user_ids[:divide_num]
            user_ids = user_ids[divide_num:]
            for userid in users:
                sessions = [uuid.uuid4() for _ in range(random.randint(3,20))]
                for sessionid in sessions:
                    # if idx == 0:
                    #     st_f.write(f"{sessionid},{faker.date_time_between(start_date='-1M', end_date='now')}\n")
                    # else:
                    st_f.write(f"{sessionid},{faker.date_time_between(start_date=f'-{MONTH_RANGE}M', end_date='now')}\n")
                    usc_f.write(f"{sessionid},{userid},{random.choice(CHANNELS)}\n")
        usc_f.close()
        st_f.close()


    def write_session_transactions(self):
        with open(session_timestamps_path, 'r') as f:
            rdr = csv.reader(f)
            next(rdr)  # skip header
            sessions = []
            for sid,_ in rdr:
                sessions.append(sid)
            with open(session_transactions_path, 'w') as stf:
                stf.write('sessionid,refunded,amount\n')
                for sid in random.sample(sessions, 1000):
                    stf.write(f'{sid},{not bool(random.randint(0,15))},{random.randint(50,150)}\n')

