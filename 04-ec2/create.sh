# Create the key if it doesn't exist.
echo "Checking for existing ddbimport.key"
if [ ! -e ddbimport.key ]
then
    echo "Creating new key pair..."
    ssh-keygen -f ddbimport.key -N ""
fi

# Upload the private key if needed.
echo "Checking for keypair in AWS..."
aws ec2 describe-key-pairs --key-names=ddbimport --output=text
if [ $? -ne 0 ]
then
    echo "Importing key pair..."
    aws ec2 import-key-pair \
        --key-name=ddbimport \
        --public-key-material=file://ddbimport.key.pub
fi

echo "Finding default VPC..."
VPCID=`aws ec2 describe-vpcs \
    --filters Name=isDefault,Values=true \
    --query 'Vpcs[*].VpcId' \
    --output text`
if [ "$VPCID" == "" ]
then
    echo "No default VPC found."
    (exit 1)
fi
echo "Using default VPC" $VPCID

echo "Checking security group exists..."
VPCSG=`aws ec2 describe-security-groups --group-names ddbimport --query 'SecurityGroups[*].[GroupId]' --output text`
if [ "$VPCSG" == "" ]
then
    echo "Creating security group..."
    VPCSG=`aws ec2 create-security-group \
        --group-name ddbimport \
        --description "ddbimport" \
        --vpc-id $VPCID \
        --output=text`
fi
echo "Using security group" $VPCSG
echo "Adding SSH access to security group..."
aws ec2 authorize-security-group-ingress \
    --group-id $VPCSG \
    --ip-permissions '[{"IpProtocol": "tcp", "FromPort": 22, "ToPort": 22, "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "SSH access from anywhere."}]}]' \
    --output=text

echo "Creating role..."
aws iam create-role --role-name ddbimport --assume-role-policy-document=file://ddbimport_role.json
echo "Attaching FullDynamoDBAccess to role..."
aws iam attach-role-policy --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess --role-name=ddbimport
echo "Creating instance profile..."
aws iam create-instance-profile --instance-profile-name ddbimport-instance-profile
echo "Adding the role to the profile..."
aws iam add-role-to-instance-profile --role-name ddbimport --instance-profile-name ddbimport-instance-profile
echo "Adding the instance profile to the instance..."
aws ec2 associate-iam-instance-profile --instance-id $INSTANCEID --iam-instance-profile Name=ddbimport-instance-profile


echo "Getting EC2 image..."
AMI=`aws ec2 describe-images --owners amazon --filters 'Name=name,Values=amzn-ami-hvm-????.??.?.????????-x86_64-gp2' 'Name=state,Values=available' --query 'reverse(sort_by(Images, &CreationDate))[:1].ImageId' --output text`
echo "Checking to see whether ddbimport is already running."
INSTANCEID=`aws ec2 describe-instances --filters 'Name=tag:Name,Values=ddbimport' --output text --query 'Reservations[*].Instances[*].InstanceId'`
INSTANCE_STATE=`aws ec2 describe-instances --filters 'Name=tag:Name,Values=ddbimport' --output text --query 'Reservations[*].Instances[*].State.Name'`
if [[ "$INSTANCEID" == "" || "$INSTANCE_STATE" != "running" ]]
then
    echo "ddbimport is not running, starting instance..."
    INSTANCEID=`aws ec2 run-instances --image-id=$AMI --instance-type t2.micro --key-name ddbimport --security-groups ddbimport --tag-specification 'ResourceType=instance,Tags=[{Key=Name,Value=ddbimport}]' --output=text --query 'Reservations[*].Instances[*].InstanceId'`
fi
echo "ddbimport has instance id" $INSTANCEID
echo "Getting public IP..."
IPADDRESS=`aws ec2 describe-instances --instance-ids $INSTANCEID --query 'Reservations[*].Instances[*].PublicIpAddress' --output text`
echo "ssh -i ddbimport.key ec2-user@" $IPADDRESS

# Copy the files
# scp -i ddbimport.key -r ../01-nodeimport ec2-user@ec2-35-178-249-154.eu-west-2.compute.amazonaws.com:/home/ec2-user
# Install Node.js
#sudo yum update -y
#curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.34.0/install.sh | bash
#. ~/.nvm/nvm.sh
#nvm install node

# Install Go
#sudo yum install golang -y

# Copy data
scp -i ddbimport.key ../data.csv ec2-user@ec2-35-178-249-154.eu-west-2.compute.amazonaws.com:/home/ec2-user
