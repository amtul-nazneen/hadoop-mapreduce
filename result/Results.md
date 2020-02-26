# Results
Implementing Hadoop/MapRedue to analyse the Social Network Data

### Question 1 
1. Write a MapReduce program in Hadoop that implements a simple “Mutual/Common friend list of two friends". The key idea is that if two people are friend then they have a lot of mutual/common friends. 
     * Input: ``soc-LiveJournal1Adj.txt`` contains the adjacency list and the ``userdata.txt`` contains dummy data 
     * Output: One line for users ``(0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)`` in the following format ``UserA,UserB TAB <Mutual/Common Friend List>``           
2. Results
     * **0,1**	5,20 
     * **1,29826**	
     * **20,28193**	1 
     * **28041,28056**	6245,28054,28061
     * **6222,19272**	19263,19280,19281,19282
     
### Question 2 
1. Find friend pairs whose common friend number are within the top-10 in all the pairs.Output them in decreasing order 
     * Input: ``soc-LiveJournal1Adj.txt`` contains the adjacency list and the ``userdata.txt`` contains dummy data 
     * Output Format: ``UserA,UserB TAB <Mutual/Common Friend List>``        
2. Results
     * **18683,18728**	99 
     * **18688,18728**	99	
     * **18698,18699**	99
     * **18685,18696**	99
     * **18683,18710**	99
     * **18676,18721**	99
     * **18681,18707**	99
     * **18695,18711**	99
     * **18676,18712**	99
     * **18722,18729**	99

### Question 3 
1. Use in-memory join to answer this question. Given any two Users (they are friends) as input, output the list of the names and the date of birth (mm/dd/yyyy) of their mutual friends. Use the userdata.txt to get the extra user information. 
     * Input: ``soc-LiveJournal1Adj.txt`` contains the adjacency list and the ``userdata.txt`` contains dummy data 
     * Output Format: ``UserA id, UserB id, list of [names: date of birth (mm/dd/yyyy)]`` of their mutual Friends.          
2. Results
     * **28041,28056**	Taylor:8/17/1973,Carl:11/10/1981,Stephanie:9/21/1965
      
### Question 4 
1. Use reduce-side join and job chaining. Calculate the maximum age of the direct friends of each user. Sort the users based on the calculated maximum age in descending order as described in step 1. Output the top 10 users with their address and the calculated maximum age. 
     * Input: ``soc-LiveJournal1Adj.txt`` contains the adjacency list and the ``userdata.txt`` contains dummy data 
     * Output Format: ``User A, 1000 Anderson blvd, Dallas, TX, 60``          
2. Results     
     * 10240, 547 Wright Court, Birmingham,Alabama, 35205, US	90
     * 1560, 4391 C Street, Worcester, Massachusetts, 1609, US	90
     * 43912, 3404 Margaret Street, Houston, Texas, 77006, US	90
     * 43918, 677 Ella Street, Palo Alto, California, 94301, US	90
     * 44049, 3887 Sardis Station, Eagan, Minnesota, 55121, US	90
     * 44066, 3353 Airplane Avenue, Hartford,Connecticut, 6182, US	90
     * 44068, 3257 Lee Avenue, Camden, New Jersey, 8104, US	90
     * 44075, 2121 Rollins Road, Big Springs, Nebraska, 69122, US	90
     * 44083, 2619 Bassel Street, Metairie, Louisiana, 70001, US	90
     * 44101, 3646 Frederick Street, El Paso, Texas, 79922, US	90
