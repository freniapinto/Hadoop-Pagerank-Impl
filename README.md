# Implementation of Pagerank in a distributed environment (Hadoop)  

# Preprocessing  
The Pre-processing job includes a Map-Reduce(to get all pages including dangling nodes and the adjacency lists) and Map job(initialize all pages with rank as 1/numberOfPages)  
The Parser.java file is a standalone program to parse input files and print in human-readable form and create a graph from the wiki dump.  
Issues:  
- Special characters in Page names of Wiki pages (handled by converting to Bytes and Latin encoding)  
- Replacing & with &amp;  
- Removed all the duplicates in adjacency list  
- If a link in an adjacency list does not have an adjacency list, made it dangling node  

# Pagerank calculation
The pagerank operation consists of 10 iterations of Map – Reduce and a final Map job to distribute delta values across all pageranks  

# Top-100
Each Mapper sends the local top 100 pages with high pagerank values. The number of reducers is set to 1 to compute the global top 100 pages.  

