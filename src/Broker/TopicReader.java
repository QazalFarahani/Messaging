package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import static logger.Logger.logger;

public class TopicReader {

    RandomAccessFile topicFile;

    private Topic topic;
    private String groupName;


    TopicReader(Topic topic, String groupName) {
        this.topic = topic;
        this.groupName = groupName;
        try {
            topicFile = new RandomAccessFile(topic.getTopicFile(), "r");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public int get(String consumerName) {
        synchronized (groupName) {
            int value = -5;
            try {
                value = topicFile.readInt();
                logger(consumerName + "  -read value : " + value);
                if (value == 0) {
                    topic.setTransaction(consumerName);
                    logger(consumerName + "  -start transaction");
                } else if (value == -1) {
                    topic.setTransaction(" ");
                    logger(consumerName + "  -finish transaction");
                }
            } catch (Exception e) {

            }
            return value;
        }
    }

}


