#!/bin/bash
#
# The purpose of this script is to import table from AYPD to Hadoop Incrementally
#
#
#
#
##########################################################################

TIMESTAMP=`date "+%Y-%m-%d"`
touch /home/$USER/logs/${TIMESTAMP}.success_log1
touch /home/$USER/logs/${TIMESTAMP}.fail_log1
success_logs=/home/$USER/logs/${TIMESTAMP}.success_log1
failed_logs=/home/$USER/logs/${TIMESTAMP}.fail_log1

#Function to get the status of the job creation
function log_status
{
       status=$1
       message=$2
echo "$status"
       if [ "$status" -ne 0 ]; then
echo $status
 echo "`date +\"%Y-%m-%d %H:%M:%S\"` [ERROR] $message [Status] $status : failed" | tee -a "${failed_logs}"
                mail -a /home/$USER/logging/"DB1_agr_event_latestTab_log" -s "This is the failed job log" ravikirandasari@gmail.com < /home/$USER/logs/${TIMESTAMP}.fail_log1
                exit 1
                else
echo $status
 echo "`date +\"%Y-%m-%d %H:%M:%S\"` [INFO] $message [Status] $status : success" | tee -a "${success_logs}"
                 mail -a /home/$USER/logging/"DB1_agr_event_latestTab_log" -s "This is the Success job log" ravikirandasari@gmail.com < /home/$USER/logs/${TIMESTAMP}.success_log1
                fi
}

echo " #### starting Sqoop Impport  ####"

sqoop job --exec DB1_EVENT_LATEST_JOB > /home/$USER/logging/"DB1_agr_event_latestTab_log" 2>&1
  g_STATUS=$?
  log_status $g_STATUS "DB1_AGR_EVENT_LATEST Table Import"
echo "import completed"

# creating temp table to filter out duplicate records 

hive -e " create TABLE aypd.DB1_agr_event_latest_temp \
as \
select database_name,country_code,versioned_agreement_number,product_name,event_type,event_purpose,package_original_start_on,\
policy_owner_met_number,policy_owner_msisdn_number,msisdn_owner_name,package_status,member_agreement_number,package_number,\
last_event_effective_at,last_is_request,last_event_id,last_last_event_rank_eq_1,load_at \
from \
(select ROW_NUMBER() over (partition by versioned_agreement_number,event_Type,event_purpose order by load_at desc) rowno,* from \
aypd.DB1_agr_event_latest)a where rowno=1;" >> /home/$USER/logging/"DB1_agr_event_latestTab_log;" 2>&1
g_STATUS=$?
  log_status $g_STATUS "DB1_AGR_EVENT_LATEST Table Import"
echo " tem table created"
echo " overwriting Base Table"

# Overwriting table by temp table, so base table will have updated records with no duplicats 

hive -e " insert overwrite table aypd.DB1_agr_event_latest select * from aypd.DB1_agr_event_latest_temp;" >> /home/$USER/logging/"DB1_agr_event_latestTab_log;" 2>&1
g_STATUS=$?
  log_status $g_STATUS "DB1_AGR_EVENT_LATEST Table Import"
echo "base table oberwrited"

#Deleteing tem table

hive -e " drop table aypd.DB1_agr_event_latest_temp purge;" >> /home/$USER/logging/"DB1_agr_event_latestTab_log;" 2>&1
echo " droped temp table"
