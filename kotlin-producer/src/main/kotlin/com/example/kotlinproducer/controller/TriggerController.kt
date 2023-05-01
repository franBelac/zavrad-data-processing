package com.example.kotlinproducer.controller

import com.example.kotlinproducer.controller.dto.Book
import com.example.kotlinproducer.service.BookLoadingService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class TriggerController(private val bookLoadingService: BookLoadingService) {

    @GetMapping("/books-list")
    fun getBooksList() = bookLoadingService.listBooks()

    @PostMapping("/analyze")
    fun analyze(@RequestBody book : Book) = bookLoadingService.loadBook(book.name)

}