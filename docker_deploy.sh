#!/bin/bash

if [[ -z "$1" || -z "$2" || -z "$3" ]]; then
	   echo "Arguments for deploydocker"
	      echo " 1. dockerDir - The directory where to the docker code is located"
	         echo " 2. dockerRepo - name of the dockor repository in AWS ECS"
		    echo " 3. dockerTag - tag for the docker image in AWS ECS"
		       echo ""
		          echo "Generates the file image.uri which contains the AWS ECS URL to pull the docker immage"
			     echo "Example URL"
			        echo "   633377509572.dkr.ecr.us-east-1.amazonaws.com/i360-flume-agents:v1.5"
				   exit
fi

dockerDir="$1"
dockerRepo="$2"
dockerTag="$3"


cd $dockerDir

aws ecr get-login --region us-east-1 > dockerlogin

filename="dockerlogin"
while read -r line
do
	    name="$line"
	        passcode=$(echo $name | cut -d' ' -f 6)
		    dockerRepoURL=${name#*"https://"}
		        dockerLogin="docker login -u AWS -p $passcode https://$dockerRepoURL"
		done < "$filename"

		echo "$dockerRepoURL"
		echo "$dockerLogin"
		eval $dockerLogin
		rm dockerlogin

	docker image prune -a -f

sudo docker build --no-cache=true -t $dockerRepo:$dockerTag .

sudo docker tag $dockerRepo:$dockerTag $dockerRepoURL/$dockerRepo:$dockerTag

echo "$dockerRepoURL/$dockerRepo:$dockerTag"
sudo docker push $dockerRepoURL/$dockerRepo:$dockerTag

echo "$dockerRepoURL/$dockerRepo:$dockerTag" > image.url
