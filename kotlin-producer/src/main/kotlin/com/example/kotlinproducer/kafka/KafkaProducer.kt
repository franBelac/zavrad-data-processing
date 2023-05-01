package com.example.kotlinproducer.kafka

import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component

@Component
class KafkaProducer(val kafkaTemplate: KafkaTemplate<String, String>) {

    fun produceMessage(message: String) {
        kafkaTemplate.send("books", message)
    }
}