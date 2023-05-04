# Databricks notebook source
# MAGIC %run ./_databricks-academy-helper $lesson="5.2"

# COMMAND ----------

# MAGIC %run ./_utility-functions

# COMMAND ----------

DA.cleanup()
DA.init()

DA.create_bronze_table()          # Clone (don't processes) the bronze table
DA.create_user_lookup()           # Create the user-lookup table
DA.create_gym_mac_logs()          # Create the gym_mac_logs()
print()

DA.process_heart_rate_silver()    # Process the heart_rate_silver table
DA.process_workouts_silver()      # Process the workouts_silver table
DA.process_completed_workouts()   # Process the completed_workouts table
DA.process_workout_bpm()          # Process the workout_bpm table
DA.process_users()
DA.process_user_bins()

DA.conclude_setup()

