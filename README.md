# Redis Synchronous Pubsub Library

## What is it ?
This is a Java Web Component (@WebListener) to send message to Redis channel (topic) and using latch to read the response from another channel.


```
                                                                             //=====================\\
                                                                             ||                      ||
/=================================\                                          || o-----------------o  ||
||                                || ----- send request --------------->     || | CHANNEL_REQUEST |  ||
|| Your Java Web Application      ||                                         || o-----------------o  ||  <==== Another system to read CHANNEL_REQUEST and publish the response to CHANNEL_RESPONSE (something like https://github.com/leonartambunan/apachecamel-in-springboot)
|| (+ Redis-Synchronous-PubSub)   ||                                         || o-----------------o  ||
||                                || <-------- you will get response ---     || | CHANNEL_RESPONSE|  ||
\=================================/                                          || o-----------------o  ||
                                                                             ||                      ||
                                                                             \\======================//
```                                                                             

## How to Use
```
....
RedisProperty.hostname="localhost";
RedisProperty.port= 6379;
RedisProperty.password="pass";//leave it blank if the redis has no authentication in place
RedisProperty.requestChannelName="CHANNEL_REQUEST";
RedisProperty.responseChannelName="CHANNEL_RESPONSE";


String request = new Date().toString();
try {
  result = SynchronousPubSub.getInstance().send(request);
} catch (Exception e) {
    e.printStackTrace();
}
....

```

## Dependencies

* javax.enterprise:cdi-api:2.0.SP1
* javax:javaee-web-api:7.0
* org.apache.commons:commons-lang3:3.11
* redis.clients:jedis:3.6.1
* javax.servlet:servlet-api:2.3
* org.slf4j:slf4j-api:1.7.32
* com.googlecode.json-simple:json-simple:1.1.1


