import boto3
import io
from collections import Counter
from ec2_metadata import ec2_metadata
import json
import time
import random

#Function to send message to an existing queue

def send_message_to_queue(messageGID,MessageDId,queue_url,message_body):
    #Create SQS client
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    response=sqs_client.send_message(MessageGroupId=messageGID,MessageDeduplicationId=MessageDId,QueueUrl=queue_url,MessageBody=message_body)
    return response

#To get text file from S3 bucket
def getfromS3():
    s3=boto3.client('s3',region_name='us-east-1')
    data=s3.get_object(Key='file.txt')
    contents=data['Body'].read().decode('utf-8')
    content=str(contents)
    return content

#Function to count words in text file
def wordCount(content):
    out_dict={}
    for text in content.splitlines():
        b=text.replace('\n','')
        line=b.split(" ")
        #print(line)
        for i in line:
	    if i.strip()!='':
            	if i in out_dict:
                    out_dict[i]+=1
            	else:
                    out_dict[i]=1

    #print(max(out_dict.values()))
    count_words=Counter(out_dict)
    #print(count_words.most_common(3))
    return out_dict

#To retreive status of EC2 instances
def getEC2status():
    ec2=boto3.resource('ec2',region_name='us-east-1')
    count=0
    array1=list()
    for instance in ec2.instances.all():
        print(instance.id)
        aa=instance.id
        array1.append(aa[2:4])
    highest=max(array1)
    print("highest is")
    print(highest)

    return highest
    #find miaximum

#To get leader node among all three nodes
def getLeader():
    highest=getEC2status()
    current=ec2_metadata.instance_id
    current=current[2:4]
    print("current is")
    print(current)
    if current==highest:
        return 1
    else:
        return 0

#Function to receive message
def receive_msg(queue_url,max_num_of_msgs,wtTime):
    sqs_client = boto3.client('sqs', region_name='us-east-1')
	response = sqs_client.receive_message(QueueUrl=queue_url,MaxNumberOfMessages=max_num_of_msgs,WaitTimeSeconds=wtTime)
    returnResponse=response['Messages'][0]
    receipt_handle =  returnResponse['ReceiptHandle']
    #Delete received message from queue
    sqs_client.delete_message(QueueUrl=queue_url,ReceiptHandle=receipt_handle)
    return returnResponse['Body']

#Main function
def main():

    //if the status of leader is 1 the split text files and send message to queue
    if leaderstatus==1:
        content=getfromS3()
        split=int(len(content)/3)
        content_leader=content[0:split]
        content_s1=content[split+1:2*split]
        content_s2=content[2*split+1:]
        leadercount=wordCount(content_leader)
        print("Leader Word count")
        count_words_master=Counter(leadercount)
        print("Most common words in Master")
        print(count_words_master.most_common(3))
        print("Sending 1")
        send_message_to_queue(messageGrpID,MessageDId,queue_url, content_s1)
        send_message_to_queue(messageGrpID1,MessageDId1,queue_url, content_s2)
        time.sleep(4)
        receive_s1json=receive_msg(queue_url,max_num_of_msgs,wtTime)
        time.sleep(4)
        receive_s2json=receive_msg(queue_url,max_num_of_msgs,wtTime)
        receive_s1=json.loads(receive_s1json)
        print("Received From first Slave")
        count_words_s1=Counter(receive_s1)
        print("Most common words in first Slave")
        print(count_words_s1.most_common(3))
        receive_s2=json.loads(receive_s2json)
        print("Received from second Slave")
        count_words_s2=Counter(receive_s2)
        print("Most common words in second Slave1")
        print(count_words_s2.most_common(3))

	//if leader status=0 the process files as per leader instructions
    else:
        msgjson=receive_msg(queue_url,max_num_of_msgs,wtTime)
	sCount=wordCount(msgjson)
        count_words_s=Counter(sCount)
        print("Most Common Words in Slave")
        print(count_words_s.most_common(3))
        print("Least Common words in Slave")
        print(count_words_s.most_common()[:-3:-1])
        sCountjson=json.dumps(sCount)
        send_message_to_queue(smessageGrpID1,sMessageDId1,queue_url, sCountjson)

if __name__ == '__main__':
    main()
