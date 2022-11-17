from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context 
from airflow.decorators import dag, task
from datetime import datetime,  timedelta
import requests 
import pandas as pd
import pandahouse as ph

# параметры DAG'a 
default_args = {
    'owner': 'v-volkova', # Владелец операции 
    'depends_on_past': False, # Зависимость от прошлых запусков
    'retries': 2, # Кол-во попыток выполнить DAG
    'retry_delay': timedelta(minutes=5), # Промежуток между перезапусками
    'start_date': datetime(2022, 10, 12)
    }

# соединение с тестовой  БД, в которую будет писаться таблица с отчетом за день 
connect_test = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': '####',
        'user': 'student-rw',
        'database': 'test'
        }
# создаем подключение к БД, из которой будем загружать данные 
connection = {
		'host': 'https://clickhouse.lab.karpov.courses',
		'database':'simulator_20220920',
		'user':'student', 
		'password':'####'
		}

# выдачу отчета организуем каждый день в 13:00 
schedule_interval = '0 13 * * *'
 

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def aaa_volk_task4():

    @task()
    def extract_actions():
        # подключились к БД и выполнили запрос по извлечению данных
        # из таблицы по действиям в ленте постов
        q1 = """
                SELECT 
                    user_id,
                    MAX(age) as age, 
                    MAX(os) as os,
                    MAX(gender) as gender,
                    sum(action = 'like') as likes,
                    sum(action = 'view') as views,
                    toString(toDate(time)) as event_date
                FROM {db}.feed_actions 
                WHERE toDate(time) = today() - 1
                GROUP BY  user_id, event_date
            """
        df = ph.read_clickhouse(q1, connection=connection)
        return df
    
    @task() 
    def extract_message():
        # подключились к БД и выполнили запрос по извлечению данных
        # из таблицы по сообщениям
        q2 = """
                SELECT user_id,
                        age, 
                        os,  gender,
                       messages_sent,
                       users_sent,
                       messages_received,
                       users_received,
                       event_date
                FROM
                  (SELECT 
                            user_id,
                            MAX(age) as age , 
                            MAX(os) as os,
                            MAX(gender) as gender,
                            count(reciever_id) as messages_sent,
                            count(distinct reciever_id) as users_sent,
                            toString(toDate(time)) as event_date
                        FROM simulator_20220920.message_actions 
                        GROUP BY  user_id, event_date      
                      HAVING toDate(time) = today() - 1) t1
                   JOIN
                     (SELECT 
                            reciever_id,
                            count(user_id) as messages_received,
                            count(distinct user_id) as users_received,
                            toString(toDate(time)) as event_date
                        FROM simulator_20220920.message_actions 
                        GROUP BY  reciever_id, event_date      
                      HAVING toDate(time) = today() - 1) t2
                   ON t1.user_id =t2.reciever_id
            """
        df = ph.read_clickhouse(q2, connection=connection)
        return df

    @task() 
    # Объединение исходных таблиц
    def transform(df_cube_feed,df_cube_mess):
        df_total1 = df_cube_feed.merge(df_cube_mess, how='outer', on=['user_id', 'event_date', 'age', 'os',  'gender'])  
        return df_total1
    
    @task() 
    # Срез по операционной системе
    def transform_os(df):
        df_os = df.groupby(['event_date','os'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_os['dimension'] = 'os'
        df_os.rename(columns = {'os' : 'dimension_value'}, inplace = True)
        return df_os
    
    @task() 
    # Срез по полу
    def transform_gender(df):
        df_g = df.groupby(['event_date','gender'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_g['dimension'] = 'gender'
        df_g.rename(columns = {'gender' : 'dimension_value'}, inplace = True)
        return df_g
    
    @task() 
    # Срез по возрасту
    def transform_age(df):
        df_age = df.groupby(['event_date','age'])\
        ['views','likes','messages_received','messages_sent','users_received','users_sent'].sum().reset_index()
        df_age['dimension'] = 'age'
        df_age.rename(columns = {'age' : 'dimension_value'}, inplace = True)
        return df_age

    @task() 
    # Объединение срезов, задание типов полей
    # даты (комментированные строки) оказалось эффективнее не определять, 
    # явное их задание вызывало проблемы при записи в clickhouse
    def concat_all(df1, df2, df3):
        df_total = pd.concat([df1, df2, df3])
        df_total = df_total.astype({
                                    #'dimension' : 'string',
                                    #'dimension_value' : 'string',
                                    #'event_date' : 'datetime64',
                                    'views': 'int32',
                                    'likes': 'int32',
                                    'messages_sent': 'int32',
                                    'users_sent': 'int32',
                                    'messages_received' : 'int32',
                                    'users_received' : 'int32'
                                   })
        return df_total
    
    @task() 
    # загрузка результирующей таблицы с данными по отчету 
    # в собственную дополняемую таблицу в базе данных   
    def load(result):
        create_table = """
                    CREATE TABLE IF NOT EXISTS test.volkova_viktor(
                        dimension String,
                        dimension_value String,
                        event_date String,
                        views Int32,
                        likes Int32,
                        messages_sent Int32,
                        users_sent Int32,
                        messages_received Int32,
                        users_received Int32)
                    ENGINE = MergeTree()
                    ORDER BY event_date
                """
        ph.execute(query=create_table, connection=connect_test)
        ph.to_clickhouse(df = result, table = 'volkova_viktor', connection=connect_test, index=False)
        
    # собственно шаги DAG'a, каждое из которых - отдельная task'a     
    df_cube_feed = extract_actions()
    df_cube_mess = extract_message()
    df_all = transform(df_cube_feed,df_cube_mess)
    df_os = transform_os(df_all)
    df_g = transform_gender(df_all)
    df_age = transform_age(df_all)
    result =  concat_all(df_os, df_g, df_age)  
    load(result)
    
aaa_volk_task4 = aaa_volk_task4()