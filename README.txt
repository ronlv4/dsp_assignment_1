Shir Saban 208013375
Ron Levi 205508021


To run our code, you just need to run localapp.jar in the following way:
java -jar localapp.jar <path to input file> <path to output file> <num of files per worker> <optional: terminate>
for example: java -jar localapp.jar ./input.txt ./output.html 3 terminate

Please be aware that you need to have a credentials file in .aws folder which is located in your home dir.
This file needs to contain an access key id, secret access key and session token.
for example: 
[default]
aws_access_key_id=aaaaa
aws_secret_access_key=bbbb
aws_session_token=cccccc

ami-id: ami-0f9fc25dd2506cf6d
we used t2.micro for all instances
it took 45 minutes to finish working on the input files
we used n=3



Our code works in the following manner:
The local app initializes one manager ec2 container (if it’s not running), writes the input file to pre made S3 bucket and a message to pre made SQS queue.
The sqs message includes the path to the file, the queue in which the local app will wait for response and the number of files per worker. It also includes the termination if needed.
The manager gets the message from the sqs queue and immediately delegates the work to another thread (from the thread pool) so he can keep listening for new requests.
The new thread reads the file from S3 and breaks it down into lines, it calculates the amount of needed workers and initializes more if needed.
Then, it places a sqs message per every line in the worker queue. The message includes the file name and analysis time, and the queue name in which it will wait for response.
There is also a property named order which helps us ignore duplicate answers (which can happen in the simple queue) and if needed, verify that the answers will be ordered.
Then, it waits for responses from the workers, waiting till it gets all the unique order properties.
After it has all of the answers it constructs an answer string and places it in S3, then sending a sqs message that the local app will receive.
The local app accesses the file from S3 and constructs an html output file.

If there is a termination property, the Manager will wait until all workers are done, then it will send n termination messages in the sqs queue.
Each worker will gracefully terminate itself and its ec2 container.
In the end the manager will also terminate its ec2 container.

How the worker works:
When it initializes, every single worker waits for messages in a Workers dedicated queue.
When a worker receives a message, including the specific attributes which are meant to fill important information regarding the 
desired analysis type, the URL of the text file, the bucket to which the output file will be saved and more.
On receiving termination message, the worker finishes parsing and terminates gracefully including its own EC2 instance.
Within execution of a worker, the file is being downloaded, analyzed, and gets deleted (a separate output file is being generated. Then the file is uploaded to s3, and a success message is sent to the manager.
Timer and visibility:
We made sure that when a worker receives a message, he immediately locks it by changing its visibility, and then using a different thread (Timer utility) constantly being updated as long as the parsing is not finished. 
Exception handling:
We made sure that any parsing error will not sabotage the whole worker. Exceptions are sent as messages to the manager as the worker resumes working on the next available message in the queue. For some cases there exist a failed worker queue, which the manager can monitor.



- We are not storing credentials in our code at all, we are using the default credential chain provider and the program is looking for a credentials file in the user’s computer.
- The program will work with any number of users, the Manager always remains available. Our bottlenecks are the workers themselves
- If a worker fails, the message will return to the sqs queue after the visibility timeout.
- We ran up to 3 clients at the same time and everything worked fine (except the runtime, which was bad because of the workers)
- In the termination process, we block any further requests from local apps.
  Then we wait for all the threads to be done (means all of the local apps are taken care of).
  Then the manager sends termination messages to all workers, which terminate gracefully, and finally the manager terminates.
- the workers are always working (if theres work).