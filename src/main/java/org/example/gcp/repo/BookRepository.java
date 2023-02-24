package org.example.gcp.repo;

import com.google.cloud.datastore.Key;
import org.example.gcp.dao.Book;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BookRepository {
    Key setBookData(String title, String author, Long counter, String sectionName);
    Book getBookByTitle(String title);
    List<Book> getBooks();
    List<Book> getBooksWithTheSameSection(String sectionName);
    String rentBook(String title);
    String sendReminder();
    void downloadDataFromGCS();
    void addBookToBigQueryTable(String title, String author, Long counter, String sectionName);
}
