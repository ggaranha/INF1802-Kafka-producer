import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Scanner;

import java.time.LocalDate;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

public class ProducerDemoInput {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithSerializer.class.getName());

    private static Scanner scn = new Scanner(System.in);

    public static void main(String[] args){

        // Criar as propriedades do produtor
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Criar o produtor
        //KafkaProducer<String,User> producer = new KafkaProducer<>(properties);
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        String str = "";

        // Enviar as mensagens
        while(str != null && !str.equals("end")) {
            /*String name = "user"+i;
            LocalDate birthday = LocalDate.now().plusDays(i);
            User userRef = new User(name, birthday);
            Tweet twt = new Tweet (userRef.name, "teste"+i, birthday );
            ProducerRecord<String,String> record = new ProducerRecord<>("tweets_topico", twt.getTweetDate() + " " + twt.getUsername() + " " + twt.getTweetText());
            //ProducerRecord<String,User> record = new ProducerRecord<>("tweets_topico", userRef);
            producer.send(record);
            /*producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e == null) {
                        logger.info("Exibindo os meta-dados sobre o envio da mensagem. \n" +
                                "Topico: " + recordMetadata.topic() + "\n" +
                                "Partição: " + recordMetadata.partition() + "\n" +
                                "Offset" + recordMetadata.offset());
                    } else {
                        logger.error("Erro no envio da mensagem", e);
                    }
                }*/
            System.out.print("Enter text: ");
            str = scn.nextLine();
            ProducerRecord<String,String> record = new ProducerRecord<>("tweets_topico", str);
            producer.send(record);
        }// Envio assíncrono
        // Fecha o produtor
        producer.close();
    }



}
