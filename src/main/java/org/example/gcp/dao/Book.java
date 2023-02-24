package org.example.gcp.dao;

import lombok.Getter;
import lombok.Setter;
@Getter
@Setter
public class Book {
    private String title;
    private String author;
    private Long counter;
    private String book_section;

    public Book(String title, String author, Long counter, String book_section) {
        this.title = title;
        this.author = author;
        this.counter = counter;
        this.book_section = book_section;
    }

    @Override
    public String toString() {
        return "Book{" +
                ", title='" + this.title + '\'' +
                ", author='" + this.author + '\'' +
                ", counter=" + this.counter +
                '}';
    }

}

