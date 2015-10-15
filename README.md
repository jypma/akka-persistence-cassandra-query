[![Build Status](https://travis-ci.org/jypma/akka-persistence-cassandra-query.svg?branch=master)](https://travis-ci.org/jypma/akka-persistence-cassandra-query)

About
=====

This is an akka persistence query implementation for the [time_index fork](https://github.com/jypma/akka-persistence-cassandra/tree/time_index)
of the akka persistence cassandra plugin, originally written by [krasserm](https://github.com/krasserm/akka-persistence-cassandra).

BUGS
====

 - Write tests for RealTime, FanoutAndMerge, etc. that show that _failures_ on any of the connected streams are propagated. 

TODO
====
