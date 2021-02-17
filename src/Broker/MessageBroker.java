package Broker;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class MessageBroker {
    private Map<String, Topic> topics = new HashMap<>();
    private TreeSet<String> groups = new TreeSet<>();

    private void addTopic(String name) {
        topics.put(name, new Topic(name));
    }

    public void put(String topic, String producerName, int value) {
        synchronized (this) {
            if (!topics.containsKey(topic)) {
                addTopic(topic);
            }

            topics.get(topic).put(producerName, value);
            groups.clear();
            notifyAll();
        }
    }

    public int get(String topic, String groupName, String consumerName) throws NoSuchTopicException {
        synchronized (this) {
            if (!topics.containsKey(topic))
                throw new NoSuchTopicException(topic);

            startTransaction(topic, consumerName);
            doNotify(topic);

            int r = topics.get(topic).get(groupName, consumerName);

            if(r == -5)
                groups.add(groupName);

            while (groups.contains(groupName)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return r;
        }
    }

    private void doNotify(String topic) {
        if (topics.get(topic).getTransaction().equals(" ")) {
            notifyAll();
        }
    }

    private void startTransaction(String topic, String consumerName) {
        while (!consumerName.equals(topics.get(topic).getTransaction()) && !topics.get(topic).getTransaction().equals(" ")) {
            try {
                wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
