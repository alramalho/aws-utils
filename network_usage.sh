#!/bin/bash
if [ $# -ne 5 ]; then
echo "Usage: $0 <REGION> <NetworkIn|NetworkOut> <START_TIMESTAMP> <END_TIMESTAMP> <PROFILE>"
echo -e "\tNote: Do not change the order of parameters."
echo -e "\n\tExample: $0 eu-west-1 NetworkOut 2020-06-01T00:00:00.000Z 2020-06-30T23:59:59.000Z my-profile"
exit 1
fi
REGION="$1"
METRIC="$2"
START_TIME="$3"
END_TIME="$4"
PROFILE="$5"

ADD_INSTANCES=""

INSTANCES="${ADD_INSTANCES} $(aws ec2 describe-instances --region ${REGION} --profile ${PROFILE} --query Reservations[*].Instances[*].InstanceId --output text)" || { echo "Failed to run aws ec2 describe-instances commandline, exiting..."; exit 1; }
[ "${INSTANCES}x" == "x" ] && { echo "There are no instances found from the given region ${REGION}, exiting..."; exit 1; }
for _instance_id in ${INSTANCES}; do
  unset _value
  _instance_name=$(aws ec2 describe-tags --region ${REGION} --profile ${PROFILE} --filters "Name=resource-id,Values=${_instance_id}" "Name=key,Values=Name" --output text | cut -f5)
  _value="$(aws cloudwatch get-metric-statistics --metric-name ${METRIC} --start-time ${START_TIME} --end-time ${END_TIME} --period 86400 --namespace AWS/EC2 --statistics Sum --dimensions Name=InstanceId,Value=${_instance_id} --region ${REGION}  --profile ${PROFILE} --output text)"
  [ "${_value}x" == "x" ] && { echo "Something went wrong while calculating the network usage of ${_instance_id}"; continue; }
  echo "${_instance_name} (${_instance_id}): $(echo "${_value}" | awk '{ sum += $2 } END {printf ("%f\n", sum/1024/1024/1024)}';) GiB";
done
echo -e "\nNote: If you think the values are inaccurate, please verify the input and modify if needed."