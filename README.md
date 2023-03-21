## Description

Find out these numbers in each month from April 2020 to April 2021.
- Total # of drop-off users
- Total # of returning users
- Total # of new users

We need to develop a special
method to set up the churn period for use in calculating drop-off users and returning users. The various
definitions are defined as follows.
- Total # of drop-off users in month M:
    - The number of users who didn’t create transactions longer than the churn
period (Days) in month M
- Total # of returning users in month M:
    - The number of drop-off users who create the transactions in month M
    - If we have the same user in condition for drop-off users and returning users in the same
month, We assume that we have 1 returning user and 1 returning user in that month.
- Total # of new users in month M:
    - The number of users who create the first transaction in month M

Note : The value of churn periods differs for each user type due to their different characteristics in using
our system. Therefore, we assign each group of users a different churn period as follows
- A = 360 Days
- B = 360 Days
- C = 120 Days
- D = 260 Days

Input:
1.  daily_transactions.csv: we have only 3 columns in this file.
    - _id: The unique id for users
    - event_date: The date that the user has created the transaction in our system
    - usertype: The type of user. [A, B, C, D]. If you found the same user who has more than 1
user type, please remove that user from the calculation.

Expected Output:
1. File CSV with 4 columns

    | month | dropoff_users  | returning_users  | new_users |
    | ------- | --- | --- | --- |
    | April 2020 | 23127 | 10293 | 3810 |
    | May 2020 | 39494 | 12752 | 3278 |
    | ... |  |  |  |
    | April 2021 | 81237 | 19804 | 8921 |

    These numbers are just mock values.

2. Source code to produce the CSV in #1
3. How to run source code to produce the CSV in #1

Additionally, you can provide additional visual representations of how the code works in the form of
diagrams.

## Environment:
Make sure you have Java and Python runtime environment set up on your machine.

- OpenJDK 11.0.8
- Python Version: 3.7
- Spark Version: 3.3.1

## Install pyspark
```
pip install pyspark
```

## How to run
Go under the main directory and run `main.py` file.
```
python main.py
```
This script will generate `result.csv` file with expected result and you can find it under the same directory.

Note: `result.csv` file will generate everytime you run the `main.py` file and the old existing file will be replaced with the new generated file.
