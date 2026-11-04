"""Reusable PySpark helpers for the optimization and data-warehousing labs.

Written to work on Databricks serverless (Free Edition), which runs Spark Connect:
no sparkContext / RDD APIs, no df.cache(), and most spark.conf keys are locked.
"""
