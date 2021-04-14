# Distributed Storage Server Project

ECE419 Distributed System

University of Toronto, Spring 2021

We spent days and nights to finish this project, a distributed storage server / hash table service implemented from scratch. There are around 6k+ lines of code, some good design descisions and some bad descisions. Feel free to explore around, but please don't copy paste directly. 

If you are too lazy to read this long document, this is probably what you are looking for. 

- [Milesstone 1](#milestone-1)
- [Milesstone 2](#milestone-2)
- [Milesstone 3](#milestone-3)
- [Milesstone 4](#milestone-4)

## Project Overview

This is a course project that implements a distributed storage service or a distributed hash-table-like system. The goal of this project is to implement a highly reliable and robust storage distributed service with the assistance of open-source tools such as ZooKeeper. 

This project provides basic hash table operations, `put(key, value)` and `get(key)`. It supports additional features such as persistent storage (milestone 1), distributed mode (milestone 2), data replication (milestone 3), crash recovery (milestone 3), etc.

## Project Architecture

This project mainly consists of three elements, which every element is capable for running as a separate process. 

### Storage Server (KVServer)

The storage server (KVServer) application is a storage database server, that contains key-value pairs data from the storage clients. It runs in two modes, stand-alone mode or distributed mode. 

Stand-alone mode KVServer is created via command line `java -jar mx-server.jar <port> 0 NONE`. The KVServer listens on `<port>` for one or multiple KVClient's requests. It assumes there is only one KVServer and one or more KVClients in the entire system. All clients share the same data in its disk storage. 

Distributed mode KVServer can only be created by ECSClient. The KVServer listens on `<port>` for one or multiple KVClient's requests. Meanwhile, KVServer also listens to ECSClient through admin interface / ZooKeeper node. In distributed mode, KVServer is capable to transfer / receive key-value pair data from / to other KVServers in the system. One KVServer is only responsible for part of the entire data space, while holding data replicas from neighbouring KVServers. 

KVServer does not have any command line prompt, since it only response to requests. 

### Storage Client (KVClient)

The storage client (KVClient) application is a storage database client, that sends requests to actively running KVServers. It embeds stand-alone mode and distributed mode so that user does not feel any difference when sending requests. 

KVClient application can only be created via command line `java -jar mx-client.jar`. Once the storage client application starts running, it then opens a command prompt where user can entre desired command. 

The list of possible commands for KVClient are:

- `connect <ip_address> <port>`: connect to a running storage server
- `disconnect`: disconnect from a storage server
- `put <key> <value>`: insert `<key, value>` pair into database
- `put <key>`: delete `<key>` from database (empty `<value>`)
- `get <key>`: query `<key>` from database
- `subscribe <key1> <key2> <...> | all` turn on data subscription for keys or all keys
- `subscribe <key1> <key2> <...> | all` turn off data subscription for keys or all keys
- `loglevel <level>`: change logger level to one of `ALL`, `INFO`, `DEBUG`, or `ERROR`
- `quit`: quit application
- `help`: show help

### External Configuration Service (ECS) Client

The external configuration service (ECS) client acts as the controller of the distributed data storage servers. The ECS client is capable for managing a pool of KVServers (a.k.a nodes) specified in `ecs.config` configuration. It is capable for creating new nodes, removing nodes, updating nodes responsibility, managing data replication and handling crash recovery. 

ECSClient application can be created via command line `java -jar mx-ecs.jar ecs.config` and then remote start distributed KVServers via SSH remote call on local or remote machines. Once the ECS client application starts running, it then opens a command prompt where user can entre desired command. 

The list of possible commands for ECS Client are:

- `addnode`: add one node to the server pool
- `addnode <number_of_nodes>`: add multiple nodes to the server pool
- `removenode <list_of_server_names>`: remove nodes from the server pool
- `start`: start all servers to responde to client requests
- `stop`: stop all servres from responding to client requests
- `shutdown`: shutdown and stop all servers 
- `status | serverstatus`: show status of all servers
- `hashring | hashringstatus`: show status of current consistent hash ring
- `loglevel <level>`: change logger level to one of `ALL`, `INFO`, or `ERROR`
- `quit`: quit application
- `help`: show help

## Project Milestones

The project is divided into four milestones with progressive functionality improvement. 

### Milestone 1

Milestone 1 implements the storage server (KVServer) and storage client (KVClient) in stand-alone mode. There is only one KVSserver in the system, and responds to multiple KVClient connections sharing the same data. The KVServer is capable to process insertion, deletion and query operations through `put(key,value)` `get(key)` requests from KVClient, and stores key-value pairs on its local disk storage. 

#### Change Log

- Implement storage server and persistent disk storage (`KVServer`).
- Implement storage client (`KVClient`, `KVStore`).
- Implement server-client communication interface through sending `KVMessage` over socket connection (`KVCommunicationServer`, `KVCommunicationClient`).
- Implement Junit tests (`ConnectionTest`, `Interactiontest`, `AdditionalTest`).
- Performance analysis for stand-alone mode (`PerformanceTest`).

#### Design Document

- [M1 Design Doc](https://docs.google.com/document/d/104ayatDv8uOkepNxO7QOZpJnh3SG_ZDeUvNjldy23_k/edit?usp=sharing)

#### TODO Tasks / Known Issue

- In-memory cache (optional) is not implemented. 

### Milestone 2

MIlestone 2 builds on top of previous milestone, and implements distributed storage server via an external configuration service (ECS) as a controller for all KVServer instances. The ECS client should be capable for adding, removing, managing KVServer nodes, and manage responsible key-value pairs for each KVServer in the system. The KVServer should listen to ECS client as well as process requests from KVClient. The KVClient should connect to the correct KVServer to insert or obtain the desired key-value pair. 

#### Change Log

- Implement ECSNode and consistent hashing logics used in ECS Client (`ECSNode`, `ECSConsistentHashRing`). 
- Implement ECS Client for adding, removing, managing, calculating and updating hash range / metadata for active KVServer nodes (`ECSClient`, `Metadata`).
- Implement ECSClient-KVServer communication through `KVAdminMessage` via ZooKeeper (`ECSClient`, `KVServer`, `KVAdminMessage`). 
- Implement KVServer distributed mode and admin interface with `KVAdminMessage`. KVServer listens to admin requests from ECSClient and other KVServers (`KVServer`, `KVAdminMessage`).
- Implement KVClient distributed mode and caching latest known metadata for connecting to the correct KVServer (`KVClient`, `KVStore`). 
- Additional Junit tests on ECS functionality (`ECSBasicTests`). 

#### TODO Tasks / Known Issue

- Remove node cannot update consistent hash ring nor transfer disk storage data (implemented in M3). 
- ECS cannot monitor KVServer crash (implemented in M3).
- Potential bug in consistent hash ring logic (found and fixed in M3). 
- No performance evaluation.
- Junit tests are partially passing.

#### Design Document

- [M2 Design Doc](https://docs.google.com/document/d/1YtnRDjRmM3C67pkyfoQS9kga9EukfQi_KA_D9nd7Lu8/edit?usp=sharing)

### Milestone 3

This milestone builds on top of milestone 1 and 2 and extends the functionality of the distributed storage server. The KVServers should replicate its data on two other KVServers. ECS should manage replicated data as well as handle failure automatically for any crashed KVServer. 

#### Change Log

- Implement data replication on the neighbouring two KVServers in consistent hash ring (`ECSConsistentHashRing`, `ECSClient`).
- Additional fields in `KVAdminMessage` and `Metadata` to achieve data replication (`KVAdminMessage`, `Metadata`). 
- Implement ZooKeeper watcher for handle crashed KVServer and failure recovery (`ECSClient`).
- Bug fixes in consistent hash ring logic (`ECSConsistentHashRing`). 
- Implement KVServer crash recovery logic when a connected KVServer goes offline (`KVClient`, `KVStore`). 
- Junit tests for newly added functionalities (`ECSConsistentHashRingTest`, `ECSCornerCasesTest`, `ECSReplicationTest`).
- Performance analysis in distributed mode (`ECSPerformanceTest`). 

#### Design Document

- [M3 Design Doc](https://docs.google.com/document/d/1MogGhpx3yABZGG98v3PF_p8JcqUesoaUj06cLddROb4/edit?usp=sharing)

#### TODO Tasks / Known Issue

- Junit tests are partially passing (some intermittent failures).

### Milestone 4

This milestone is an open ended improvement to the key-value store service and it builds on top of all previous milestones. This milestone implements a sequential consistency model and data subscription mechanism.

#### Change Log

- Stricter data-centric consistency with sequential model such that KVServer only responds to `PUT` messages after replicas finish updating the key-value pair (`KVServer`, `KVCommunicationServer`, `KVAdminMessage`).
- Additional KVMessage type `SUBSCRIPTION_UPDATE`. When client `PUT` a key-value pair, responsible server forwards the update to all peer servers, and then each server boardcasts message to all connected clients (`KVMessage`, `KVMessageClass`, `KVCommunicationServer`, `KVServer`, `KVCommunicationClient`).
- Multi-threaded KVClient implementation for data subscription mechanism. One sender thread that sends requests upon user command, one listener thread that constantly listens to server messages, including data subscription update. (`KVClient`, `KVStore`, `KVCommunicationClient`). 
- Modified KVClient command line interface, two commands are added, `subscribe` and `unsubscribe` (`KVClient`, `KVStore`). 
- Bug fix for imtermittent test failures, KVServer delete zNode during graceful shutdown (`KVServer`). 
- Junit tests for newly added functionalities (`StrictConsistencyTest`, `DataSubscriptionTest`).
- Updated previous unit tests to adapt `KVStore` API change.

#### Design Document

- [M4 Design Doc](https://docs.google.com/document/d/1PXZrNNJxUkVuipkByj4yJjFWdSzMxqJ_o99Vupzv-Qo/edit?usp=sharing)

#### TODO Tasks / Known Issue

- Refactor and cleanup code base
- Performance optimization

## Build and Run Instructions 

### Required Environment

#### Java 11

This project is developed and tested only on Java 11 with Apache Ant as building tool. 

#### ZooKeeper

This project requires [Apache ZooKeeper v3.4.11](https://zookeeper.apache.org/doc/r3.4.11/releasenotes.html), disregards that zookeeper `v3.6` is included in `libs` directory. The program always assumes that a ZooKeeper server is running at runtime. 

You may find ZooKeeper package tgz file [here](./zookeeper-3.4.11.tar.gz). Unzip the package, move to project directory, and run ZooKeeper server in the background. Replace "`x`" with a milestone number, one of 2, 3, or 4.

``` bash
tar -xf zookeeper-3.4.11.tar.gz
cp zookeeper-3.4.11 milestonex/zookeeper-3.4.11
./zookeeper-3.4.11/bin/zkServer.sh start
```

#### Working Directory Path

The ECS program uses a variable `serverDir` to store current working directory and launch KVServer instances. The defaul value is set to `System.getProperty("user.dir")`. This could be incorrect on MacOS. You can select to hard code this value to the absolute path of your working directory (location of `mx-ecs.jar`).

#### SSH Access

The ECS program uses `ssh` to launch KVServer instances even if these programs runs on the same machine. Make sure that SSH keys are setup and port 22 is opened on your laptop.

### Build Instruction

Replace "`x`" with a milestone number, one of 1, 2, 3, or 4. 

``` bash
cd milestonex
ant cleanall
ant build-jar
```

### Run Instruction

- Run external configuration service (ECS)

    Replace "`x`" with a milestone number, one of 2, 3, or 4. The ECS program expects a ECS configuration file in below format. You may find an example [here](./milestone3/ecs.config). 
    ``` conf
    # ecs.config
    server_name_1 ip_address port_number
    server_name_2 ip_address port_number
    ...
    ```
    ``` bash
    java -jar mx-ecs.jar <configuration_file>
    ```
    This provides an ECS command prompt, refer to previous guide or type `help` to see all avaliable commands. 

- Run Storage Server (KVServer)

    KVServer can be launched by ECS client in distributed mode, or launched by below command as non-distributed mode. 

    Replace "`x`" with a milestone number, one of 1, 2, 3, or 4. The KVServer program takes three arguments, port number that KVServer listens on, cache size (only support `0`, no cache), cache strategy (only support `NONE`, no cache).
    ``` bash 
    java -jar mx-server.jar <port> <cache_size> <cache_strategy>
    ```
    The KVServer program does not have a command prompt, it only listens to client requests (both modes) and ECS requests (distributed mode). 

- Run Storage Client (KVClient)

    Replace "`x`" with a milestone number, one of 1, 2, 3, or 4. 
    ``` bash
    java -jar mx-client.jar
    ```
    This provides a KVClient command prompt, refer to previous guide or type `help` to see all avaliable commands. 

## Contributors

Thanks for the contribution and sleepless nights spent together with my teammates. 

- [@shanchao95](https://github.com/shanchao95)
- [@cwangCharlie](https://github.com/cwangCharlie)
- [@chrisc66](https://github.com/chrisc66)
