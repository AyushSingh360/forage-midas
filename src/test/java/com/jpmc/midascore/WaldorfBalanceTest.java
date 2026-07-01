package com.jpmc.midascore;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
public class WaldorfBalanceTest {

    @Autowired
    private KafkaProducer kafkaProducer;

    @Autowired
    private UserPopulator userPopulator;

    @Autowired
    private FileLoader fileLoader;

    @Autowired
    private UserRepository userRepository;

    @Test
    void calculateWaldorfBalance() throws InterruptedException {
        userPopulator.populate();
        String[] transactionLines = fileLoader.loadStrings("/test_data/mnbvcxz.vbnm");
        for (String transactionLine : transactionLines) {
            kafkaProducer.send(transactionLine);
        }
        Thread.sleep(5000);

        List<UserRecord> users = (List<UserRecord>) userRepository.findAll();
        for (UserRecord user : users) {
            System.out.println("User: " + user.getName() + ", Balance: " + user.getBalance());
        }

        UserRecord waldorf = userRepository.findById(5L);
        System.out.println("\n=== WALDORF BALANCE: " + waldorf.getBalance() + " ===");
        System.out.println("Rounded down: " + (int) Math.floor(waldorf.getBalance()));
    }
}
