package main

import (
    "bufio"
    "context"
    "fmt"
    "github.com/parnurzeal/gorequest"
    "os"
    "sync"
    "time"
)

const workersCount = 24

func getUrlWorker(ctx context.Context, urlChan <-chan string, wg *sync.WaitGroup) {
    defer wg.Done()
    for {
        select {
        case <-ctx.Done():
            return
        case url, ok := <-urlChan:
            if !ok {
                return
            }
            resp, body, errs := gorequest.New().Get(url).End()
            if len(errs) > 0 {
                fmt.Printf("Error fetching %s: %v\n", url, errs)
            } else {
                _ = resp
                _ = body
            }
        }
    }
}

func main() {
    start := time.Now()

    txtfile, err := os.Open("top-1m.txt")
    if err != nil {
        fmt.Printf("Error opening file: %v\n", err)
        return
    }
    defer txtfile.Close()

    scanner := bufio.NewScanner(txtfile)

    urlChan := make(chan string, workersCount)
    ctx, cancel := context.WithCancel(context.Background())
    var wg sync.WaitGroup

    for i := 0; i < workersCount; i++ {
        wg.Add(1)
        go getUrlWorker(ctx, urlChan, &wg)
    }

    completed := 0

    for scanner.Scan() {
        domain := scanner.Text()
        url := fmt.Sprintf("http://%s", domain)
        select {
        case urlChan <- url:
        case <-ctx.Done():
            break
        }
        completed++
        fmt.Printf("Completed %s (%d)\n", url, completed)
    }

    if err := scanner.Err(); err != nil {
        fmt.Printf("Error reading file: %v\n", err)
    }

    close(urlChan)
    wg.Wait()
    cancel()

    fmt.Println("Completed Transactions:", completed)
    fmt.Println("Time Elapsed:", time.Since(start))
}
