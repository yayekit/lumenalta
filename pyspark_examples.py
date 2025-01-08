from pyspark.sql import functions as F
from pyspark.sql.window import Window

def analyze_customer_spending(df):

    # convert to monhnly spending
    monthly_spending = df.withColumn(
        'month', F.date_trunc