# BigData-Final-Project
## Author - Neelesh Saladi

## Objective
Process text using Databricks Community Edition and PySpark.

## Text data
Source for Text Data [The Project Gutenberg eBook of The Wonderful Wizard of Oz, by L. Frank Baum
](https://www.gutenberg.org/files/55/55-0.txt)

- I took a ebook from Project Gutenberg for this project, and I used the text gutenberg ebook of The Wonderful Wizard of Oz, by L. Frank Baum, with pySpark. First, I will pull the data. After that, I will clean the data, then process it, and finally, I will graph the most frequently used terms.

## Tools/Languages
- Languages: Python
- Tools: Notebooks
- Databricks Cloud Environment.
- Spark Processing Engine.
- PySpark.
- Pandas
- Regex
- Matplotlib
- Urllib.

## Databrick community
Here we will be using Databrick community for running the commands. First we have to create a cluster, then after creating it we have to go to the notebooks and create a new notebook with language selected as python.Then after that we have to enter the commands and run it.

- https://community.cloud.databricks.com/?o=8572756908237677#notebook/702988485285366/command/4364163144246798

## Process and Commands Used


```
import urllib.request
urllib.request.urlretrieve("https://www.gutenberg.org/files/55/55-0.txt" , "/tmp/Neelesh.txt")
```
```
dbutils.fs.mv("file:/tmp/Neelesh.txt","dbfs:/data/Neelesh.txt")
```
```
Neelesh_RDD = sc.textFile("dbfs:/data/Neelesh.txt")
```

### Data Cleaning :
- The text is currently in book form with capitalization, punctuation, sentences, and stopwords. Stopwords are just words that make a sentence flow better but don't add anything to the sentence. For example "a", To get the word count the first step is to flatmap and get rid of capitalization and spaces. Flatmapping is just breaking up the sentences into words. The next step is to move all the punctuation, this can be done by using the regular expression,by importing the re. The last step is to remove the stopwords by using the import the library StopWordsRemover from pyspark. Then , now we will filter out the words. now, remove all the empty sets using filter keyword.

```
wordRDD=Neelesh_RDD.flatMap(lambda line : line.lower().strip().split(" "))
```
```
import re
tokenCleanerRDD = wordRDD.map(lambda w: re.sub(r'[^a-zA-Z]','',w))
```
```
from pyspark.ml.feature import StopWordsRemover
remover =StopWordsRemover()
stopwords = remover.getStopWords()
cleanedwordRDD=tokenCleanerRDD.filter(lambda w: w not in stopwords)
```
```
cleanedwordRDD=tokenCleanerRDD.filter(lambda w: w not in stopwords)
```
### Data Processing :
- For Data Processing, the first step is to Map words to key-value pairs.Here, we need to change the words into the form(word,1), then we count the occurancy of the word.Next step is by using the ReduceByKey, we need to remove the duplicate words occured in the text and add the first count words.The last step is to return to the python from the spark using Collect( ) function.

```
IKVPairsRDD= cleanedwordRDD.map(lambda word: (word,1))
```
```
wordsCountRDD = IKVPairsRDD.reduceByKey(lambda acc, value: acc+value)
```
```
results = wordsCountRDD.collect()
```

### Charting :
- The final processing is to display the final output data and then  visualize our performance using MatPlotLib, and Seaborn.Viewing a list of words is fine but it is better to graph data. To create a graph we will use the library mathplotlib. Here is a helpful stack overflow on how to graph a list of tuple using one side of the x axis and the other side for y axis.

```
results.sort(key=lambda x:x[1])
results.reverse()
print(results[:12])
```
```
mostCommon=results[1:14]
word,count = zip(*mostCommon)
import matplotlib.pyplot as plt
fig = plt.figure()
plt.stackplot(word,count, color='blue')
plt.xlabel("No of times used")
plt.ylabel("Most repeated words")
plt.title("Most used words in the File")
plt.show()
```

- Sorting the words in the descending order and printing the results to check the first 25 results in descending order.

```
final_results = Final_word_count_RDD.map(lambda x: (x[1], x[0])).sortByKey(False).take(25)
print(final_results)
```
- collect() action to get back to python

```
results = Final_word_count_RDD.collect()
print(results)
```
## Charting the results:

- We use pandas, matplotlib seaborn to chart the results.

```
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from collections import Counter

source = 'The Project Gutenberg eBook of The Wonderful Wizard of Oz, by L. Frank Baum'
title = 'Top Words in ' + source
xlabel = 'Count'
ylabel = 'Words'

# create Pandas dataframe from list of tuples
df = pd.DataFrame.from_records(final_results, columns =[xlabel, ylabel]) 
print(df)

# create plot (using matplotlib)
plt.figure(figsize=(15,15))
sns.barplot(xlabel, ylabel, data=df, palette="Paired").set_title(title)
```

## Results
![](https://github.com/neeleshsaladi/BigData-Final-Project/blob/main/b4fc1621-8e57-4b67-83c1-01008eb39b73.png)

![](https://github.com/neeleshsaladi/BigData-Final-Project/blob/main/c5d84b5c-3199-49bc-a961-8769000a26e6.png)


## References :
- [MatplotLib](https://matplotlib.org/stable/tutorials/introductory/sample_plots.html)
- https://www.w3schools.com/python/python_lambda.asp
- https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/3328674740105987/4033840715400609/6441317451288404/latest.html
