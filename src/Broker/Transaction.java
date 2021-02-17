package Broker;

import java.util.LinkedList;
import java.util.Queue;

import static logger.Logger.logger;

public class Transaction {

    private TopicWriter topicWriter;
    private String producerName;
    private Queue<Integer> values;

    Transaction(TopicWriter topicWriter, String producerName) {
        this.topicWriter = topicWriter;
        this.producerName = producerName;
        values = new LinkedList<>();
    }

    void put(int value) {
        values.add(value);
    }

    void commit() {
        int value;
        topicWriter.writeValue(0);
        while(!values.isEmpty()) {
            value = values.remove();
            topicWriter.writeValue(value);
            logger(producerName + "  -write value : " + value);
        }
        topicWriter.writeValue(-1);
    }
}
