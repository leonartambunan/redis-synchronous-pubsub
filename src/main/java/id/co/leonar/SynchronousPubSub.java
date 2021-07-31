package id.co.leonar;


import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Logger;

@WebListener
public class SynchronousPubSub implements ServletContextListener {
    private Logger LOG = Logger.getLogger(SynchronousPubSub.class.getName());

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        init();
        instance = this;
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        destroy();
    }

    private static SynchronousPubSub instance;

    public static SynchronousPubSub getInstance() {
        return instance;
    }

    Map<String, String> mapRequestIdToMessage = new ConcurrentHashMap<String, String>();

    public static final String REDIS_CHANNEL_REQUEST = RedisProperty.requestChannelName;
    public static final String REDIS_CHANNEL_RESPONSE = RedisProperty.responseChannelName;

    Map<String, CountDownLatch> mapRequestIdToLatch = new ConcurrentHashMap<>();

    private CountDownLatch addLatchForRequest(String requestId) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        mapRequestIdToLatch.put(requestId, countDownLatch);
        return countDownLatch;
    }


    private final JedisPubSub jedisPubSub = new JedisPubSub() {
        @Override
        public void onMessage(String channel, String incomingMessage) {
            if (REDIS_CHANNEL_RESPONSE.equals(channel)) {
                LOG.info(incomingMessage);
                JSONParser jsonParser = new JSONParser();
                try {
                    JSONObject json =  (JSONObject) jsonParser.parse(incomingMessage);
                    String requestId = (String) json.get("requestId");
                    String message = (String) json.get("message");
                    CountDownLatch countDownLatch = mapRequestIdToLatch.get(requestId);
                    if (countDownLatch != null) {
                        LOG.info("Redis pub/sub onMessage got message "+incomingMessage+", continue thread");
                        mapRequestIdToLatch.remove(requestId);

                        System.out.println("Put message to Map : "+message);
                        mapRequestIdToMessage.put(requestId, message);
                        countDownLatch.countDown();
                    } else {
                        System.out.println("SYSTEM ERROR latch not found for request id "+requestId+", mapRequestIdToLatch keyset " + mapRequestIdToLatch.keySet());
                    }

                } catch (ParseException e) {
                    LOG.warning(e.getMessage());
                }
            }
        }
    };


    private ExecutorService subscriberExecutor;
    private ExecutorService publisherExecutor;
    private Future subscriberFuture;
    private synchronized void init() {
        jedisSubscriber = new Jedis(RedisProperty.hostname,RedisProperty.port);
        if (StringUtils.isNotEmpty(RedisProperty.password)) {
            jedisSubscriber.auth(RedisProperty.password);
        }

        jedisPublisher = new Jedis(RedisProperty.hostname,RedisProperty.port);
        if (StringUtils.isNotEmpty(RedisProperty.password)) {
            jedisPublisher.auth(RedisProperty.password);
        }
        if (subscriberFuture == null) {
            LOG.info("Start background thread for getting responses from backend through Redis");
            subscriberExecutor = Executors.newFixedThreadPool(1);
            subscriberFuture = subscriberExecutor.submit(redisListenerTask);
            publisherExecutor = Executors.newFixedThreadPool(20);
        }
    }

    private RedisListenerTask redisListenerTask = new RedisListenerTask();

    public synchronized void destroy() {
        if (subscriberFuture != null) {
            try {
                jedisPubSub.unsubscribe();
                subscriberExecutor.shutdownNow();
                publisherExecutor.shutdownNow();
                LOG.info("Background thread for getting responses through Redis closed");
            } catch (Exception ex) {
                LOG.warning(ex.getMessage());
                ex.printStackTrace();
            } finally {
                try {
                    jedisPublisher.quit();
                    jedisSubscriber.quit();
                } catch (Exception e) {
                }
            }
        }
    }


    private class RedisListenerTask implements Runnable {

        public void run() {
            try {
                jedisSubscriber.subscribe(jedisPubSub, REDIS_CHANNEL_RESPONSE);
            } catch (Exception ex) {
                LOG.warning(ex.getMessage());
                ex.printStackTrace();
            }
        }
    }

    private Jedis jedisPublisher;
    private Jedis jedisSubscriber;

    public String send(String payload) {

        Callable<String> callable = () -> {

            try {
                JSONObject json = new JSONObject();
                String requestId = UUID.randomUUID().toString();

                json.put("requestId",requestId);
                json.put("message",payload);

                jedisPublisher.publish(REDIS_CHANNEL_REQUEST, json.toJSONString());
                System.out.println(">>Publish request " + payload + " to Redis on using " + REDIS_CHANNEL_REQUEST+ " with requestId "+requestId);

                addLatchForRequest(requestId);

                CountDownLatch countDownLatch = mapRequestIdToLatch.get(requestId);
                if (countDownLatch != null) {
                    System.out.println("Publishing message with requestId "+requestId+" continue thread");

                    System.out.println();
                    System.out.println("\nPUBLISH TEST_OUT '{\"requestId\":\""+requestId+"\",\"message\":\""+payload +" responded\"}'");
                    System.out.println();

                    try {
                        boolean completed = countDownLatch.await(20000, TimeUnit.MILLISECONDS);
                        if (completed) {
                            System.out.println("completed");
                            System.out.println("requestId:"+requestId);
                            return getMessageByRequestId(requestId);
                        }
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                        e.printStackTrace();
                    }
                } else {
                    System.out.println("SYSTEM ERROR latch not found for request id   "+requestId+"   mapRequestIdToLatch keyset "+ mapRequestIdToLatch.keySet());

                }
                return "timeout";
            } catch (Exception ex) {
                LOG.warning(ex.getMessage());
                ex.printStackTrace();
                return  ex.getMessage();
            }
        };


        String result = "timeout";
        try {
            Future<String> future = publisherExecutor.submit(callable);

            System.out.println("Do something else while callable is getting executed");

            System.out.println("Retrieve the result of the future");
            // Future.get() blocks until the result is available

            result = future.get();
            System.out.println(result);
//            publisherExecutor.shutdown();
        }catch (Exception e) {
            e.printStackTrace();
        }

        return result;

    }

    private String getMessageByRequestId(String requestId) {
        return mapRequestIdToMessage.remove(requestId);
    }

}
