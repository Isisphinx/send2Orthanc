package main

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	pb "github.com/cheggaaa/pb/v3"
	"github.com/dgraph-io/badger"
	"github.com/karrick/godirwalk"
)

func post(file []byte, job string) bool {
	client := http.Client{Timeout: time.Second * 2}
	res, err := client.Post("http://root:1234@10.111.12.20/instances", "application/dicom", bytes.NewBuffer(file))
	if err != nil {
		return false
	}
	fmt.Println(job, res.StatusCode)
	defer res.Body.Close()
	return res.StatusCode == http.StatusOK
}

func deleteKeySlice(db *badger.DB, deleteKey []string) {
	err := db.Update(func(txn *badger.Txn) error {
		for _, d := range deleteKey {
			err := txn.Delete([]byte(d))
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		log.Println("Delete err", err)
	}
}
func dropKeyPrefix(db *badger.DB, prefix string) {
	collectSize := 10000
	err := db.View(func(txn *badger.Txn) error {
		var deleteKey []string
		keysCollected := 0
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefixByte := []byte(prefix)
		for it.Seek(prefixByte); it.ValidForPrefix(prefixByte); it.Next() {
			item := it.Item()
			key := string(item.Key())
			deleteKey = append(deleteKey, key)
			keysCollected++
			if keysCollected == collectSize {
				deleteKeySlice(db, deleteKey)
				keysCollected = 0
				deleteKey = nil
			}
		}
		if keysCollected > 0 {
			deleteKeySlice(db, deleteKey)
			keysCollected = 0
			deleteKey = nil
		}
		return nil
	})
	if err != nil {
		log.Println("Iterate err", err)
	}
}
func main() {
	var err error
	db, err := badger.Open(badger.DefaultOptions("C:/Users/Thibaut/badger"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	totalFileNumber := 0

	optVerbose := flag.Bool("verbose", false, "Print file system entries.")
	flag.Parse()
	dirname := "."
	if flag.NArg() > 0 {
		dirname = flag.Arg(0)
	}

	dropKeyPrefix(db, "todo:")

	log.Println("Todo cleared")

	err = godirwalk.Walk(dirname, &godirwalk.Options{
		Callback: func(osPathname string, de *godirwalk.Dirent) error {
			if !de.IsDir() {
				totalFileNumber++
				err := db.Update(func(txn *badger.Txn) error {
					_, err := txn.Get([]byte("done:" + osPathname))
					// Check if Key exist by checking error Key not found
					if err.Error() == "Key not found" {
						err = txn.Set([]byte("todo:"+osPathname), []byte(""))
						if err != nil {
							return err
						}
					}
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					log.Println(err)
				}
			}
			return nil
		},
		ErrorCallback: func(osPathname string, err error) godirwalk.ErrorAction {
			if *optVerbose {
				fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
			}
			return godirwalk.SkipNode
		},
		Unsorted: true,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	log.Println("number of files", totalFileNumber)
	bar := pb.StartNew(totalFileNumber)

	worker := func(jobChan <-chan string) {
		for job := range jobChan {
			bar.Increment()
			fmt.Println(job)
			file, err := ioutil.ReadFile(job)
			if err != nil {
				log.Println(err)
			}
			posted := post(file, job)
			if posted {
				err := db.Update(func(txn *badger.Txn) error {
					err := txn.Set([]byte("done:"+job), []byte(""))
					if err != nil {
						return err
					}
					err = txn.Delete([]byte("todo:" + job))
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					log.Println(err)
				}
			}
		}
	}

	jobChan := make(chan string, 1000)
	for i := 0; i < 5; i++ {
		go worker(jobChan)
	}

	// Start reading keys to send to workers
	err = db.View(func(txn *badger.Txn) error {

		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		prefixByte := []byte("todo:")
		for it.Seek(prefixByte); it.ValidForPrefix(prefixByte); it.Next() {
			item := it.Item()
			key := string(item.Key())
			path := strings.TrimLeft(key, "todo:")
			jobChan <- path
		}

		return nil
	})
	if err != nil {
		log.Println("Iterate err", err)
	}
	bar.Finish()
}

// Sources
// Drop all prefixed keys : https://github.com/dgraph-io/badger/issues/598
// https://www.opsdash.com/blog/job-queues-in-go.html
// https://stackoverflow.com/questions/37454236/net-http-server-too-many-open-files-error
// https://stackoverflow.com/questions/29529926/http-post-data-binary-curl-equivalent-in-golang/29531306
