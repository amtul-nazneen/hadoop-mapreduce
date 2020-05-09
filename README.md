# Hadoop/MapReduce
Using hadoop/mapreduce to analyze social network data

## Getting Started
These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. 


### Softwares/SDKs
1. Jdk 1.7.0
2. Any IDE
3. Hadoop Cluster Setup


### Steps to Run
1. Open Project in any IDE
2. Generate the jars. One jar for each Main Class under the ``src\app\hadoop`` package
     * Example: ``q1.jar`` for ``MutualFriends`` Main Class
3. Place the input files in the ``hadoop hdfs system``
     * Command: ``hdfs dfs -mkdir /anazneen``
     * Command: ``hdfs dfs -mkdir /anazneen/input``
     * Command: ``hdfs dfs -put userdata.txt /anazneen/input/``
     * Command: ``hdfs dfs -put soc-LiveJournal1Adj.txt /anazneen/input/``
4. Run the commands below and view the corresponding folder for the output
5. MutualFriends 
     * Command: ``hadoop jar <jar> MutualFriends <path_to soc-LiveJournal1Adj.txt> <output_folder_path>``
              * Example: ``hadoop jar q1.jar MutualFriends /anazneen/input/soc-LiveJournal1Adj.txt /anazneen/output_q1_1``  
6. Top 10 Common Friends
     * Command: ``hadoop jar <jar> Top10CommonFriends <path_to soc-LiveJournal1Adj.txt> <temp_output_folder_path> <output_folder_path>``
              * Example: ``hadoop jar q2.jar Top10CommonFriends /anazneen/input/soc-LiveJournal1Adj.txt /anazneen/output_q2_temp_1 /anazneen/output_q2_1``   
7. In Memory Join             
     * Command: ``hadoop jar <jar> InMemoryJoin <path_to soc-LiveJournal1Adj.txt> <path_to userdata.txt> <output_folder_path> <user1_id> <user2_id>``
              * Example: ``hadoop jar q3.jar InMemoryJoin /anazneen/input/soc-LiveJournal1Adj.txt /anazneen/input/userdata.txt /anazneen/output_q3_1 11 13``   
8. Reduce Side Join              
     * Command:``hadoop jar <jar> ReduceSideJoin <path_to soc-LiveJournal1Adj.txt> <path_to userdata.txt> <temp_output_folder1_path> <temp_output_folder2_path> <output_folder_path>``
              * Example: ``hadoop jar q4.jar ReduceSideJoin /anazneen/input/soc-LiveJournal1Adj.txt /anazneen/input/userdata.txt /anazneen/output_q4_temp1_1 /anazneen/output_q4_temp2_1 /anazneen/output_q4_1``   
9. To view the output do the following
     * Command:``hdfs dfs -get <output_folder_path>``
              * Example: ``hdfs dfs -get /anazneen/output_q4_1``         
     * Command:``cat <output_folder_path>/part-r-00000``
              * Example: ``cat /anazneen/output_q4_1/part-r-00000``         
