# BAC2_Couchbase
## About this Repo
* This is the code used for the Couchbase part in my second bachelor thesis.
* There is also a [MongoDB](https://github.com/hoeselm/BAC2_MongoDB/) and a [MySQL](https://github.com/hoeselm/BAC2_MySQL/) part.
* The [thesis](http://www.hoeselm.at/BachelorThesis2_Markus_Hoesel.pdf) itself is written in German. You can find the abstract below.

## Abstract of the Thesis
To satisfy the needs of massive scalability regarding to parallel read and  write queries new database systems have evolved. This paper introduces document stores and compares them with traditional relational databases. It outlines the pros and cons of both system types and gives  a  comparison  on  the  most  common  data  models,  which  are  the  embedded  and referenced document model for the document stores and normalized and denormalized table model  for  the  relational  databases. This  paper  also  discusses  transactions  and  how  to implement  them  in  the  application  layer  in  case  the  database  does  not have a support for transactions. A  test  environment  is  set  up  and  a  sample  application  is  used  in  order  to measure  the  response  time  of  the  systems.  MongoDB  and  Couchbase  are  used  as representatives for  document  stores  and  MySQL  in  combination  with  InnoDB,  MyISAM  and NDB  as  storage  engines  is  used  as  a  representative  for  relational  databases.  The  sample application  mimics  a  heavily  used  blogging  platform  with  users  writing  and  reading  blog entries  and  comments  and  making  use  of  a  “Like”  button.  The  results  show that the embedded  document  data  model  as  well  as  the  denormalized  table  data  model  are  much more  responsive  to  read  queries  then  the  referenced  document  data  model  and  the normalized  table  model,  but  with  a  serious  performance  bottleneck  on  write  queries. The normalized  table  data  model  shows  the  worst  performance.  Furthermore  the  denormalized table model has to store the same information multiple times, which can lead to inconsistent data. Because the blogging platform does not have the requirement to use transactions, the best  database  solution  for such a  big  data  application  is  the  document  store  with  a combination of the embedded and referenced data model. Therefore the paper confirms the positive impact of document stores to the current database market. 
