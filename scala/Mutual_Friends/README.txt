##### READ ME######

The Project Mutual Friends performs the Operations on the data set soc-LiveJournal1Adj.txt(Details about the user and his friends list)
and userdata.txt(Details about an individual user).

#Instructions to Execute:

1.Create a Maven project in IDE 

2.Add Scala Nature Replace the pom.xml file with the file attached in this folder

3.Place all the .scala files in the /src/main/scala folder
 
1.The MutualFriendsList.scala implements implements a simple “Mutual/Common friend list of two friends". The key idea is that if two people are friend then they have a lot of mutual/common friends. This program will find the common/mutual friend list for them.

For example,
Alice’s friends are Bob, Sam, Sara, Nancy
Bob’s friends are Alice, Sam, Clara, Nancy
Sara’s friends are Alice, Sam, Clara, Nancy
As Alice and Bob are friend and so, their mutual friend list is [Sam, Nancy]
As Sara and Bob are not friend and so, their mutual friend list is empty. (In this case you may exclude them from your output). 

Run the file as SCALA APPLICATION and pass the input file soc-LiveJournal1Adj.txt as argument.
mutualFriendsList.scala soc-LiveJournal1Adj.txt


2. The MutualFriends.scala implements to find mutual friends of 2 users. Given user1 and user2 it returns the mutual friends list. 
Run the file MutualFriends.scala as SCALA APPLICATION and pass user1 user2,soc-LiveJournal1Adj.txt as arguments when prompted.


3. The InMemoryJoin.scala Given any two Users (they are friend) as input, output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends.

Run the file InMemoryJoin.scala as SCALA APPLICATION enter the  user1 user2 userdata.txt soc-LiveJournal1Adj.txtas arguments when prompted

4. The AverageAge.scala Calculates the maximum age of the direct friends of each user.Sorts the users by the calculated maximum age in descending order and Outputs the top 10 users from step 2 with their address and the calculated maximum age.Run the file AverageAge.scala as Scala Application and pass the input files as arguments when prompted

AverageAge.scala soc-LiveJournal1Adj.txt userData.txt
