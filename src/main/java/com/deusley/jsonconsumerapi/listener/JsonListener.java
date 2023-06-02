package com.deusley.jsonconsumerapi.listener;

import com.deusley.jsonconsumerapi.model.Payment;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Log4j2
@Component
public class JsonListener {

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "create-group", containerFactory = "jsonContainerFactory")
    public void firewall(@Payload Payment payment) {
        log.info("Pagamento recebido {}", payment.toString());
        Thread.sleep(1000);

        log.info("validando...");
        Thread.sleep(2000);

        log.info("compra aprovada!");
        Thread.sleep(1000);

    }
    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "pdf-group", containerFactory = "jsonContainerFactory")
    public void pdfGenerator(@Payload Payment payment) {

        log.info("Gerando PDF id {}", payment.getId());
        Thread.sleep(1000);
    }

    @SneakyThrows
    @KafkaListener(topics = "payment-topic", groupId = "email-group", containerFactory = "jsonContainerFactory")
    public void sendEmail() {

        log.info("Enviando email de confirmação!...");
    }}