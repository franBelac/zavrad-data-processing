package com.example.kotlinproducer.service

import com.example.kotlinproducer.controller.dto.BooksList
import com.example.kotlinproducer.kafka.KafkaProducer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.io.BufferedReader
import java.io.InputStreamReader

@Service
class BookLoadingService @Autowired constructor (
    private val resourceLoader: ResourceLoader,
    private val kafkaProducer: KafkaProducer
) {

    fun listBooks(): BooksList {
        val resource = resourceLoader.getResource("classpath:books/")
        return BooksList(
            resource.file.listFiles()?.map { it.nameWithoutExtension } ?: emptyList()
        )
    }

    fun loadBook(bookName: String) {
        val resource = resourceLoader.getResource("classpath:books/$bookName.txt")
        val inputStream = resource.inputStream
        val reader = BufferedReader(InputStreamReader(inputStream))
        kafkaProducer.produceMessage(reader.use { it.readText() })
    }
}