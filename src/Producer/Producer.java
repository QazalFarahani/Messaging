package Producer;

import Broker.MessageBroker;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Producer extends Thread {
    private MessageBroker messageBroker;
    private String topicName;
    private String producerName;
    private File producerFile;

    Producer(MessageBroker messageBroker, String topicName, String producerName, File producerFile) {
        this.messageBroker = messageBroker;
        this.topicName = topicName;
        this.producerName = producerName;
        this.producerFile = producerFile;
    }

    public void run() {
        try {
            Scanner scanner = new Scanner(producerFile);
            while (scanner.hasNext()) {
                put(scanner.nextInt());
            }

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Scanner scanner1 = new Scanner(producerFile);
            while (scanner1.hasNext()) {
                put(scanner1.nextInt());
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    private void put(int value) {
        messageBroker.put(topicName, producerName, value);
    }
}
