package com.example.kotlinproducer.controller.dto

data class BooksList (
    val books : List<String>,
    val message : String = "Please select one of the books to analyze the text."
)