package lib

import (
	"log"
	"os"
	"bufio"
	"bytes"
	"io"

)

func loadCampaignData() {
	log.Println("loading data from txt")
}


func readLines(path string)(lines [] string,err error){  
	var (  
		file *os.File  
		part [] byte  
		prefix bool  
	)  
	  
	if file, err = os.Open(path); err != nil {  
		return  
	}  
	  
	reader := bufio.NewReader(file)  
	buffer := bytes.NewBuffer(make([]byte,1024))  
	  
	for {  
	   if part, prefix, err = reader.ReadLine();err != nil {  
		   break  
	   }  
	   buffer.Write(part)  
	   if !prefix {  
		  lines = append(lines,buffer.String())  
		  buffer.Reset()  
	   }  
	}  
	if err == io.EOF {  
	   err = nil  
	}  
	return  
 }  