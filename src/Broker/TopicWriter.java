package Broker;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;

import static logger.Logger.logger;

public class TopicWriter {
    RandomAccessFile buffer;

    private Topic topic;
    private HashMap<String, Transaction> transactions;

    TopicWriter(Topic topic) {
        this.topic = topic;
        transactions = new HashMap<>();
        try {
            buffer = new RandomAccessFile(topic.getTopicFile(), "rw");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void put(String producerName, int value) {
        if (value <= 0) {
            handleTransactionOperation(producerName, value);
        } else {
            handleInsertOperation(producerName, value);
        }
    }

    private void handleTransactionOperation(String producerName, int value) {
        switch (value) {
            case 0:
                startTransaction(producerName);
                break;
            case -1:
                commitTransaction(producerName);
                break;
            case -2:
                cancelTransaction(producerName);
        }
    }

    private void handleInsertOperation(String producerName, int value) {
        if (transactions.containsKey(producerName)) {
            transactions.get(producerName).put(value);
        } else {
            writeValue(value);
            logger(producerName + "  -write value : " + value);
        }
    }

    private void addTransaction(String producerName) {
        transactions.put(producerName, new Transaction(this, producerName));
    }

    /**
     * This method is used to start a transaction for putting a transaction of values inside the buffer.
     *
     * @return Nothing.
     */
    private void startTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            logger(producerName + "  error-last transaction hasn't been closed");
            commitTransaction(producerName);
            transactions.remove(producerName);
        }
        addTransaction(producerName);
        logger(producerName + "  -start transaction");
    }

    /**
     * This method is used to end the transaction for putting a its values inside the file.
     *
     * @return Nothing.
     */
    private void commitTransaction(String producerName) {
        synchronized (topic) {
            if (transactions.containsKey(producerName)) {
                transactions.get(producerName).commit();
                transactions.remove(producerName);
                logger(producerName + "  -commit transaction");
            } else {
                logger(producerName + "  error-there isn't any transaction to commit");
            }
        }
    }

    /**
     * This method is used to cancel a transaction.
     *
     * @return Nothing.
     */
    private void cancelTransaction(String producerName) {
        if (transactions.containsKey(producerName)) {
            transactions.remove(producerName);
            logger(producerName + "  -cancel transaction");
        } else {
            logger(producerName + "  error-there isn't any transaction to cancel");
        }
    }

    public void writeValue(int value) {
        synchronized (topic) {
            try {
                buffer.writeInt(value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
