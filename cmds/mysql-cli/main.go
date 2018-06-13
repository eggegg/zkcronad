package main


import (
	"log"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)


type Category struct {
	ID int `json:"id"`
	Title string `json:"title"`
}

func main() {
	log.Println("start mysql-cli ...")

	db, err := sql.Open("mysql", "homestead:secret@tcp(127.0.0.1:33060)/zk_dsp")
    if err != nil {
        panic(err.Error())
	}
	db.Ping()
	defer db.Close()
	
	results, err := db.Query("SELECT id, title FROM mini_apps_option")
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	for results.Next() {
		var category Category
		// for each row, scan the result into our tag composite object
		err = results.Scan(&category.ID, &category.Title)
		if err != nil {
			panic(err.Error()) // proper error handling instead of panic in your app
		}
                // and then print out the tag's Name attribute
		log.Printf(category.Title)
	}
	

}