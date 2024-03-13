# Building your own Load Balancer

### A load balancer performs the following functions:

* Distributes client requests/network load efficiently across multiple servers
* Ensures high availability and reliability by sending requests only to servers that are online
* Provides the flexibility to add or subtract servers as demand dictates

## Type of Load Balancing:

In the world of load balancing we have two approaches:

### Static:

Static load balancing does not incorporate the current state of back end servers: In other words it's like
"Hey man i see you are alive, cheers!! have a request"

There are three main static algorithms:

* Round-robin: distributes the traffic to the load balancers in rotation, each request going to the next backend server in the list. Once the end of the list is reached, it loops back to the first server.
* Weighted round-robin: each server is given a weight, allowing some severs to receive more of the traffic than others.
* IP hash: the client’s IP address is hashed and the hash is used to determine which server to send the request to.

### Dynamic:

Dynamic load balancing kinda self-explanatory. 
Yes you have guessed it right, It does take the state of back end servers into consideration when routing the request.

There are four main dynamic algorithms:

* Least connection: requests are routed to the server that has the fewest current open connections.
* Weighted least connection: each server has a weight - perhaps representing it’s capacity - and requests are forwarded to the least connected server, weighted against the server’s weighting.
* Weighted response time: a running average of the response time for a request is maintained for each server and the requests are routed to the server with the current lowest average response time.
* Resource-based: distributes the request based on information about the servers current CPU, memory or network load.

## It's Coding time !!

Nope wait, As a good developer we must first start with the requirements of the system. In this case they are:

* Build a load balancer that can send traffic to two or more servers.
* Health check the servers.
* Handle a server going offline (failing a health check).
* Handle a server coming back online (passing a health check).

Seems like a static algorithm like round-robing can be used for this.

Cools, now think for a while, make a conceptual diagram of each and every component and how they will interact. 
In even simpler terms, yeah make a class diagram you nerd.

Alright, So we have a picture in our mind or on a paper(preferably). We must think of how we will implement this. 
What tech stack we will use. 

So i have chosen Java + spring web flux. for the load balancer.
For backend servers, we can use inbuilt python web servers: [see this](https://codingchallenges.fyi/blog/web-server-python/)

Also, we will be dealing with a highly concurrent system. So be familiar with the concept of concurrency, Asynchronous event handling and threads.
I would recommend going through these articles.
* [Concurrency in JAVA](https://medium.com/@soyjuanmalopez/conquering-concurrency-in-spring-boot-strategies-and-solutions-152f41dd9005)
* [Spring web flux](https://www.baeldung.com/spring-webflux)

### Code flow

So i have created a controller class and 3 end-points:
* To hit the load balancer
* To add a server
* To remove a server

Now for storing and retrieving the server to pass the request to we need a data structure that is suitable for such operations:
* you can simply create a list and use a counter%list_size and counter++ to get the server. only problem here would be that upon changing the list_size the output of counter%list_size can change too.
* Another option is to use circular linked-list (Implemented).

I have also initialized the list of servers from a config file, you can take any approach you may like:
* Backend server can register themselves to loadbalancer by hitting a register endpoint with its unique id and address
* you can add a server by hitting add-server endpoint providing a unique serverid and address

For health check you can start a deamon thread that will keep on running. or you can use @Scheduled annotation in spring boot to declare a method that will run in the background
I opt for creating a background thread that will keep on running.
```java
private void startHealthCheck()
    {
        if(backgroundThread==null || !backgroundThread.isAlive())
        {
            running=true;
            backgroundThread = new Thread(()->{
                while(running)
                {
                    try{
                        for(int i=0;i<config.getAddress().size();i++)
                        {
                            HealthCheckRunnable runnable = new HealthCheckRunnable(restTemplate,list,config,i);
                            new Thread(runnable).start();
                        }
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        running = false;
                    }
                }
            });
            backgroundThread.start();
        }
    }
```

And in this i am creating a thread for each server that will check if the server is up and responding and remove and add it back to the server list.

```java
public void run() {
        String responge=null;
        try{
            responge= template.getForObject(config.getAddress().get(t),String.class);
        }catch(Exception e)
        {
            logger.error(e.getMessage());
        }

        if(responge!=null)
        {
            logger.info("Server is up "+config.getId().get(t));
            list.addBackServer(t);
        }
        else {
            logger.info("Calling server out "+ config.getId().get(t));
            try {
                list.removeServer(config.getId().get(t));
            } catch (ServerNotFoundException e) {
                logger.error(e.getMessage());
            }
        }
    }
```

So this is how i implemented a simple load-balancer utilizing round-robin algorithm with a health-check mechanism.

In the next stage of this we will implement a sticky session load-balancer with consistent hashing algorithm.