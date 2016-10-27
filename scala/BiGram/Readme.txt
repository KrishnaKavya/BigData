This project is to find Top N bi-gram words from a document whose frequencies are above a certain number.  

For example, for the following raw text, we want to apply bi-gram analysis.
“Alice is testing spark application. Testing spark is fun”. 

Applying bi-gram will generate token like this:   
[(Alice, 'is'), ('is', 'testing'), ('testing', ‘spark’), (‘spark’, 'application.'), ('testing', ‘spark’), (‘spark’, 'is'), (‘is’, 'fun')]. 

So, if we want to find top 2 frequent bi-gram tokens whose frequencies are above 1, the output will be:
('testing', ‘spark’)   2

In this program, you have also remove the stop words and stemming before applying your bi-gram logic for your document. For example,

“Testing spark is fun” will look like “test spark be fun”.

After stemming, your output of bi-gram will be like following:

Document:

“Alice is testing spark application. Testing spark is fun”. 

After removing stop words, stemming and applying bi-gram will generate tokens like this:  

 [(Alice, 'be'), ('be', 'test'), ('test', ‘spark’), (‘spark’, 'application.'), ('test', ‘spark’), (‘spark’, 'be'), (‘be’, 'fun')]. 

So, if we want to find top 2 frequent bi-gram tokens whose frequencies are above 1, the output will be:
('test', ‘spark’)   2


#Instructions to Execute:

1.Create a Maven project in IDE 

2.Add Scala Nature Replace the pom.xml file with the file attached in this folder

3.Place all the .scala files in the /src/main/scala folder

4. BiGram.scala 

Run BiGram.scala as Scala Application with input file and the frequency as the arguments when prompted

BiGram.scala input.txt 2 2

//input is a sample input text file. it five top 2 bigrams of frequency greater than or equal 2
