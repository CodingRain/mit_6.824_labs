package main

import (
	"fmt"
)

/**
Exercise: Web Crawler

In this exercise you'll use Go's concurrency features to parallelize a web crawler.

Modify the Crawl function to fetch URLs in parallel without fetching the same URL twice.

Hint: you can keep a cache of the URLs that have been fetched on a map, but maps alone are not safe for concurrent use!
**/

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// coordinator uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func coordinator(url string, depth int, fetcher Fetcher) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	foundUrls := make(map[string]bool)
	ch := make(chan []string)
	go func() {
		ch <- []string{url}
	}()
	workerCnt := 1
	for urls := range ch {
		// receiving something from signal means one worker is done
		workerCnt--

		for _, u := range urls {
			if foundUrls[u] {
				continue
			}
			foundUrls[u] = true
			fmt.Printf("found: %s\n", u)
			go func(u string) {
				_, urls, err := fetcher.Fetch(u)
				if err != nil {
					fmt.Println(err)
					ch <- []string{}
				} else {
					ch <- urls
				}
			}(u)
			workerCnt++
		}

		if workerCnt == 0 {
			close(ch)
		}
	}
	return
}

func main() {
	coordinator("https://golang.org/", 4, fetcher)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	// fmt.Printf("llf debug, url=%s\n", url)
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
