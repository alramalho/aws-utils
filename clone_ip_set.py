import argparse
import boto3

def copy_waf_ip_set(source_region, target_region, ip_set_name, profile, cf):
    boto3.setup_default_session(profile_name=profile)
    # Step 1: Initialize clients for source and target regions
    source_client = boto3.client('wafv2', region_name=source_region)
    target_client = boto3.client('wafv2', region_name=target_region)

    # Step 2: Retrieve the IP Set details from the source region
    try:
        ip_set_response = source_client.list_ip_sets(Scope='REGIONAL')
        ip_set = next((ip for ip in ip_set_response['IPSets'] if ip['Name'] == ip_set_name), None)
        if not ip_set:
            raise ValueError(f"IP Set '{ip_set_name}' not found in source region {source_region}.")

        ip_set_id = ip_set['Id']
        ip_set_details = source_client.get_ip_set(Name=ip_set_name, Scope='REGIONAL', Id=ip_set_id)
        ip_addresses = ip_set_details['IPSet']['Addresses']
    except Exception as e:
        print(f"Error retrieving IP Set from source region: {e}")
        return

    # Step 3: Create a new IP Set in the target region
    if cf:
        scope = 'CLOUDFRONT'
    else:
        scope = 'REGIONAL'
        
    try:
        create_response = target_client.create_ip_set(
            Name=ip_set_name,
            Scope=scope,
            IPAddressVersion=ip_set_details['IPSet']['IPAddressVersion'],
            Addresses=[]
        )
        target_ip_set_id = create_response['Summary']['Id']
        print(f"Created new IP Set '{ip_set_name}' in target region {target_region} and with ID {target_ip_set_id}.")
    except Exception as e:
        print(f"Error creating IP Set in target region: {e}")
        return

    # Step 4: Update the new IP Set with addresses
    try:
        target_client.update_ip_set(
            Name=ip_set_name,
            Scope=scope,
            Id=target_ip_set_id,
            LockToken=create_response['Summary']['LockToken'],
            Addresses=ip_addresses
        )
        print(f"Successfully copied IP Set '{ip_set_name}' to target region {target_region}.")
    except Exception as e:
        print(f"Error updating IP Set in target region: {e}")
        return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Clone IP set from one region to another")
    parser.add_argument("-s", "--source", type=str, help="Source region", required=True)
    parser.add_argument("-t", "--target", type=str, help="Target region. (CF is in `us-east-1`)", required=True)
    parser.add_argument("-n", "--name", type=str, help="Name of the IP set", required=True)
    parser.add_argument("-p", "--profile", type=str, help="Profile name for AWS configuration", default='default')
    parser.add_argument("-c", "--cloudfront", type=bool, help="Create in cloudfront?", default=False)
    args = parser.parse_args()

    copy_waf_ip_set(args.source, args.target, args.name, args.profile, args.cloudfront)
    print("Filtered import complete!")
