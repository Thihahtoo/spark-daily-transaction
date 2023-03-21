from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from datetime import datetime
import shutil


def remove_multiple_usertype(df):
    dis_df = df.select(['_id','usertype']).distinct().groupBy(['_id']).count()
    duplicate_id_lst = dis_df.select('_id').filter(dis_df['count']>1).rdd.map(lambda x: x._id).collect()
    df = df[~df['_id'].isin(duplicate_id_lst)]
    return df

def get_new_user(df):
    first_event_df = df.groupBy('_id').agg(F.min('event_date').alias("first_event"))
    join_df = df.join(first_event_df, df['_id']==first_event_df['_id'], 'inner').drop(first_event_df._id)
    newuser_df = join_df.withColumn("new_user", join_df["event_date"]==join_df["first_event"]).withColumn('month', F.date_format(join_df["event_date"], "MMMM yyyy"))
    newuser_count_df = newuser_df.filter(newuser_df["new_user"]==True).groupBy("month").count().withColumnRenamed("count","new_users")
    return newuser_count_df

def get_returning_user(df, churn_period_df):
    windowSpec  = Window.partitionBy("_id").orderBy("event_date")
    previous_event_df = df.withColumn("previous_event", F.lag("event_date",1).over(windowSpec)).na.drop()
    date_diff_df = previous_event_df.withColumn('date_diff', F.datediff(previous_event_df['event_date'], previous_event_df['previous_event']))
    join_period_df = date_diff_df.join(churn_period_df, date_diff_df['usertype']==churn_period_df['usertype'], 'inner').drop(churn_period_df['usertype'])
    return_user_df = join_period_df.withColumn("return_user", join_period_df["date_diff"]>=join_period_df["period"]).withColumn('month', F.date_format(df["event_date"], "MMMM yyyy"))
    return_count_df = return_user_df.select(['month', '_id']).distinct().filter(return_user_df["return_user"]==True).groupBy("month").count().withColumnRenamed("count","returning_users")
    return return_count_df

def get_dropoff_user(df, churn_period_df):
    windowSpec = Window.partitionBy("_id").orderBy("event_date")
    max_date = df.select(F.max('event_date'))
    next_event_df = df.withColumn("next_event", F.lead("event_date",1).over(windowSpec)).crossJoin(max_date).withColumnRenamed('max(event_date)', 'max_date')
    next_event_df = next_event_df.select(['_id','event_date','usertype',F.coalesce(next_event_df['next_event'], next_event_df['max_date']).alias('next_event')])
    date_diff_df = next_event_df.withColumn('date_diff', F.datediff(next_event_df['next_event'], next_event_df['event_date']))
    join_period_df = date_diff_df.join(churn_period_df, date_diff_df['usertype']==churn_period_df['usertype'], 'inner').drop(churn_period_df['usertype'])
    dropped_user_df = join_period_df.withColumn("dropped", join_period_df["date_diff"]>=join_period_df["period"]).withColumn("period", join_period_df.period.cast('int'))
    dropped_dt_df = dropped_user_df.withColumn("dropped_date", F.date_add(dropped_user_df['event_date'], dropped_user_df['period'])).filter(dropped_user_df["dropped"]==True)
    dropped_mn_df = dropped_dt_df.withColumn('month', F.date_format(dropped_dt_df["dropped_date"], "MMMM yyyy"))
    dropped_count_df = dropped_mn_df.select(['month', '_id', 'dropped']).distinct().groupBy("month").count().withColumnRenamed("count","dropoff_users")
    return dropped_count_df


if __name__ == '__main__':

    spark=SparkSession.builder.appName('RentSpree').getOrCreate()

    ## read the dataset
    df = spark.read.option('header','true').csv('daily_transactions/daily_transactions.csv',inferSchema=True)

    ## Remove Id with more than one usertype
    df = remove_multiple_usertype(df)

    ## get new user counts
    newuser_count_df = get_new_user(df)

    ## create churn period dataframe
    data = [("A",360), ("B",360), ("C",120), ("D",260)]
    churn_period_df = spark.createDataFrame(data=data,schema=['usertype', 'period'])
    
    ## get returning user counts
    return_count_df = get_returning_user(df, churn_period_df)

    ## get dropoff user counts
    dropped_count_df = get_dropoff_user(df, churn_period_df)


    ## join all dataframes
    result_df = dropped_count_df.join(return_count_df, dropped_count_df['month']==return_count_df['month'], 'outer').withColumn('c_month', F.coalesce(dropped_count_df['month'], return_count_df['month'])).drop('month')
    result_df = result_df.join(newuser_count_df, result_df['c_month']==newuser_count_df['month'], 'outer').withColumn('c_month', F.coalesce(result_df['c_month'], newuser_count_df['month'])).drop('month')
    result_df = result_df.withColumnRenamed('c_month', 'month').na.fill(0).select(['month', 'dropoff_users', 'returning_users', 'new_users'])


    ## Sort and filter by date range
    from_date = 'April 2020'
    to_date = 'April 2021'

    from_date = datetime.strptime(from_date, "%B %Y").date()
    to_date = datetime.strptime(to_date, "%B %Y").date()

    result_df = result_df.withColumn('month_num',F.to_date(result_df['month'],"MMMM yyyy"))
    result_df = result_df.filter(F.col('month_num').between(from_date, to_date)).sort(result_df['month_num']).drop('month_num')

    ## Write final result to csv
    result_df.coalesce(1).write.csv(path="result", header=True, mode="overwrite")
    file_name=list(filter(lambda x: x.endswith('.csv'), shutil.os.listdir('result')))[0]
    shutil.move(f'result/{file_name}', 'result.csv')
    shutil.rmtree('result', ignore_errors=True)
