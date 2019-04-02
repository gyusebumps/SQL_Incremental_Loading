# SQL_Incremental_Loading

Author : Gyubeom Kim

# Summary
Before physically creating the ETL script for incremental loading, I carefully started to look the tables first. Then, I compared values between the Source and Target tables. Then, I was possible to insert rows to the target tables where new rows are found in the source table, update rows in the destination that are changed in the source and delete rows from the destination that are removed in the source (Root, 2018). For this process, the different type of SDC is needed. It tracks the status of change in each row or column. Unlike the previous assignment, we did not need to drop the foreign key constraints and clear the tables. However, we have to make an abstraction layer, a view, for the incremental loading process.
