package org.example.gcp.controllers;

import com.google.cloud.datastore.Key;
import lombok.AllArgsConstructor;
import org.example.gcp.cloudtask.CreateTask;
import org.example.gcp.dao.Book;
import org.example.gcp.gcs.GCSUpload;
import org.example.gcp.service.BookService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;


import java.io.IOException;
import java.util.List;


@RestController
@AllArgsConstructor
public class BookController {

    private final BookService service;
    private final CreateTask createTask;
    private final GCSUpload GCSUpload;

    @PostMapping("/setBookData")
    public Key setBookData(@RequestBody Book book) {
        return service.setBookData(book.getTitle(),book.getAuthor(),book.getCounter(),book.getBook_section());
    }
    @GetMapping ("/listBooks")
    public List<Book> listBooks() {
        return service.getBooks();
    }

    @GetMapping("/getBookByTitle/{title}")
    public Book getBookByTitle(@PathVariable("title") String title){
        return service.getBookByTitle(title);
    }

    @GetMapping("/getBooksBySection/{sectionName}")
    public List<Book> getBooksBySection(@PathVariable("sectionName") String sectionName) {
        return service.getBooksWithTheSameSection(sectionName);
    }

    @GetMapping("/rentBook/{title}")
    public String rentBook(@PathVariable("title") String title){
       return service.rentBook(title);
    }

    @GetMapping("/remind")
    public String remind(){
        return service.sendReminder();
    }

    @GetMapping("/triggerImport")
    public void  triggerImport() throws Exception {
        createTask.addTaskForReadingFromGCS();
    }
    @PostMapping("/setDataFromCsv")
    public void  setDataFromCsvGCS() {
        service.downloadDataFromGCS();
    }

    @PostMapping(value = "/uploadFileToGCS/{fileName}",consumes = MediaType.ALL_VALUE)
    public void uploadFileToGcs(@PathVariable("fileName")  String fileName, @RequestParam("file") MultipartFile file) throws IOException {
        GCSUpload.uploadFile("arc2-366516","arc2-366516.appspot.com","booksFiles/"+fileName,file.getBytes(),file.getContentType());
    }

    @PostMapping("/addBookToBigQueryTable")
    public void setBookDataToBQ(@RequestBody Book book)  {
        service.addBookToBigQueryTable(book.getTitle(),book.getAuthor(),book.getCounter(),book.getBook_section());
    }

}
