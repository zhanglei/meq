# MeQ  [mi:kju]

A modern messaging platform for MQ、Message Push、IM、IoT etc.

Our goal is to be the best messaging platform in the world, like the iphone to others.It will have the easiest way for using, the extremely high performance, fitting for  diversity scenario, and the most beatiful interface for administrator ui.

MeQ is written in pure go and standard library,nearly no messy dependencies. so you can easily deploy a standalone binary in linux、unix、macos、windows.



Develop status
---
V0.3.0 was released on 2018.5.13, V0.5.0 will be released before 2018.5.20

Design Goals
------------
- Extremly Performanced: Zero allocation、Low Latency
- HA and Scale out
- support Message Push 、MQ、IM、IoT scenario
- Topic fuzzing match
- Message trace by **Opentracing**
- Multi persistent engine supported
- Ops friendly
 

Performance(early stage)
-------------
In this benchmark, I use the memory engine, all is done in my macbook pro laptop.
- A client with 5 goroutine can publish 2700K messages to meq per second
- A client with 5 goroutine can consume 2000K messages from meq per second

Architecture
------------

![](MeQ.jpeg)


