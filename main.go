package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/redmask-hb/GoSimplePrint/goPrint"
	"github.com/tj/go-spin"
)

const (
	DUMP    = "dump"
	RESTORE = "restore"

	MINPAGE      = 2000
	WORKER       = 201
	MAX_CAPACITY = WORKER * 4
)

type Job struct {
	Key   string
	Value []byte
}

func DoRestore(pool *redis.Pool, ch chan *Job, wg *sync.WaitGroup) {
	conn := pool.Get()
	defer conn.Close()

	for kp := range ch {
		_, e := conn.Do("restore", kp.Key, 0, kp.Value, "replace")
		if e != nil {
			fmt.Println("\rrestore err:" + e.Error())
			go makeRestoreWorker(pool, ch, 1, wg)
			break
		}
	}

	wg.Done()
}

func DoDump(pool *redis.Pool, ch chan *Job, dump_ch chan *Job, wg *sync.WaitGroup) {
	conn := pool.Get()
	defer conn.Close()

	for kp := range ch {
		dump, e := redis.Bytes(conn.Do("dump", kp.Key))
		if e != nil {
			fmt.Printf("\rdump key [%s] err: %s", kp.Key, e.Error())
			go makeDumpWorker(pool, ch, dump_ch, 1, wg)
			break
		}

		kp.Value = dump
		dump_ch <- kp
	}

	wg.Done()
}

func makeRestoreWorker(pool *redis.Pool, ch chan *Job, cnt int, wg *sync.WaitGroup) {
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go DoRestore(pool, ch, wg)
	}
}

func makeDumpWorker(pool *redis.Pool, ch chan *Job, dump_ch chan *Job, cnt int, wg *sync.WaitGroup) {
	for i := 0; i < cnt; i++ {
		wg.Add(1)
		go DoDump(pool, ch, dump_ch, wg)
	}
}

func makeRestoreJob(values map[string][]byte, ch chan *Job) {
	bar := goPrint.NewBar(len(values))
	bar.SetGraph("=")
	count := 0

	for k, v := range values {
		ch <- &Job{
			Key:   k,
			Value: v,
		}
		count++
		bar.PrintBar(count)
	}
	bar.PrintBar(count)
	bar.PrintEnd("restore key ok.")

	for len(ch) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	close(ch)
}

func makeDumpJob(match string, pool *redis.Pool, ch chan *Job) {
	conn := pool.Get()
	defer conn.Close()

	iter := 0
	total, e := redis.Int(conn.Do("dbsize"))
	if e != nil {
		fmt.Println("get dbsize err:" + e.Error())
		return
	}

	count := 0
	bar := goPrint.NewBar(total)
	bar.SetGraph("=")

	for {
		arr, e := redis.Values(conn.Do("scan", iter, "match", match, "count", MINPAGE))
		if e != nil {
			fmt.Println("scan err:" + e.Error())
			break
		}

		iter, _ = redis.Int(arr[0], e)
		keys, _ := redis.Strings(arr[1], e)
		for _, v := range keys {
			ch <- &Job{
				Key:   v,
				Value: nil,
			}

			count++
			bar.PrintBar(count)
		}

		if iter == 0 {
			break
		}
	}
	bar.PrintBar(count)
	bar.PrintEnd("search key ok.")

	for len(ch) > 0 {
		time.Sleep(10 * time.Millisecond)
	}
	close(ch)
}

func StartLoading(msg string, sp *spin.Spinner) {
	isLoading = true
	go func() {
		for isLoading {
			fmt.Printf("\r%s %s ", msg, sp.Next())
			time.Sleep(100 * time.Millisecond)
		}
		fmt.Printf("\r%s       \n", msg)
	}()
}

func StopLoading(msg string) {
	isLoading = false
	time.Sleep(200 * time.Millisecond)
	fmt.Printf("%s\n", msg)
}

var isLoading bool

func main() {
	pwd := flag.String("P", "", "password")
	host := flag.String("h", "localhost", "host")
	port := flag.Int("p", 6379, "redis port")
	db := flag.Int("n", 0, "redis database index")
	file := flag.String("o", "redis.dump", "dump or restore file path")
	keys := flag.String("k", "*", "key fiter")
	mode := flag.String("m", "dump", "support dump or restore")
	flag.Parse()

	if *mode != DUMP && *mode != RESTORE {
		fmt.Println("not support mode, only support dump or restore")
		return
	}

	n_option := redis.DialDatabase(*db)
	pwd_option := redis.DialPassword(*pwd)
	pool := redis.Pool{
		MaxIdle:   10,
		MaxActive: WORKER,
		Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", fmt.Sprintf("%s:%d", *host, *port), n_option, pwd_option)
		},
	}
	defer pool.Close()

	start := time.Now()
	var wg sync.WaitGroup
	ch := make(chan *Job, MAX_CAPACITY)
	dump_ch := make(chan *Job, MAX_CAPACITY)
	values := make(map[string]([]byte), MINPAGE)
	sp := spin.New()
	sp.Set(spin.Spin1)

	if *mode == DUMP {
		makeDumpWorker(&pool, ch, dump_ch, WORKER-1, &wg)
		go makeDumpJob(*keys, &pool, ch)

		go func() {
			for kp := range dump_ch {
				values[kp.Key] = kp.Value
			}
		}()

		wg.Wait()
		time.Sleep(time.Second)
		for len(dump_ch) > 0 {
			time.Sleep(10 * time.Millisecond)
		}
		close(dump_ch)

		ow, ferr := os.Create(*file)
		if ferr != nil {
			fmt.Println("open Write file[%s" + *file + "] err.")
			return
		}
		defer ow.Close()

		StartLoading("ready to write file, please wait.", sp)
		jerr := json.NewEncoder(ow).Encode(values)
		if jerr != nil {
			fmt.Println("write file err: " + jerr.Error())
			return
		}
		StopLoading("write file ok.")

	} else {
		makeRestoreWorker(&pool, ch, WORKER, &wg)

		of, err := os.Open(*file)
		if err != nil {
			fmt.Println("read file err:" + err.Error())
			return
		}
		defer of.Close()

		StartLoading("ready to read file, please wait.", sp)
		err = json.NewDecoder(of).Decode(&values)
		if err != nil {
			fmt.Println("read dump file err: " + err.Error())
			return
		}
		StopLoading("read file completely.")

		go makeRestoreJob(values, ch)
		wg.Wait()
	}

	end := time.Now()
	fmt.Println("total spent " + fmt.Sprintf("%v", end.Sub(start).Milliseconds()) + " ms.")
}
