package main

import "os"
import "io"
import "fmt"
import "flag"
import "sync"
import "bufio"
import "strings"
import "strconv"
import "net/url"
import "net/http"
import "github.com/montanaflynn/stats"

type cliOptions struct {
	stdout      bool
	maxLines    int
	concurrency int
}

func main() {
	options := cliOptions{}
	flag.BoolVar(&options.stdout, "stdout", true, "print results to stdout")
	flag.IntVar(&options.maxLines, "max-lines", -1, "the maximum amount of lines to display")
	flag.IntVar(&options.concurrency, "concurrency", 30, "the amount of in-flight requests to allow at a time")
	flag.Parse()

	if leftover := flag.Args(); len(leftover) != 1 {
		fmt.Printf("must provide a single filename, received %v. usage:\n", leftover)
		flag.PrintDefaults()
		os.Exit(1)
	}

	if options.concurrency <= 0 {
		fmt.Printf("concurrency must be positive, found %d", options.concurrency)
		flag.PrintDefaults()
		os.Exit(1)
	}

	filename := flag.Args()[0]

	if s, e := os.Stat(filename); e != nil || s.IsDir() {
		fmt.Printf("must provide a valid filename, found %s (error: %v)", filename, e)
		flag.PrintDefaults()
		os.Exit(1)
	}

	reader, e := os.Open(filename)

	if e != nil {
		fmt.Printf("must provide a valid filename, found %s (error: %v)", filename, e)
		flag.PrintDefaults()
		os.Exit(1)
	}

	fmt.Println("loading file")
	duplicates := make(map[string]bool)
	results := make(chan *ravenResult)
	wg := &sync.WaitGroup{}
	fetchers := make(chan *http.Client, options.concurrency)

	for i := 0; i < options.concurrency; i++ {
		fetchers <- &http.Client{}
	}

	processor := resultProcessor{
		results: results,
		queue:   fetchers,
	}

	semo := make(chan struct{})
	rollup := flockMetrics{
		sizes:     make([]float64, 0, 1e3),
		ambiguous: make([]error, 0, 10),
	}

	go func() {
		received := 0

		for raven := range results {
			received += 1
			fmt.Printf("%d received (%s)\n", received, raven.url)
			rollup.add(raven)
		}

		semo <- struct{}{}
	}()

	leader := 0

	for line := range iter(reader, options.maxLines) {
		_, present := duplicates[line]
		leader += 1

		if present {
			fmt.Printf("%d duplicate %v\n", leader, line)
			continue
		}

		url, e := url.Parse(line)

		if e != nil {
			fmt.Printf("line: %s had no valid url, skipping (e %v)\n", line, e)
			continue
		}

		fmt.Printf("%d fetching %v\n", leader, url)

		duplicates[line] = true
		wg.Add(1)
		go processor.fetch(url, wg)
	}

	reader.Close()
	wg.Wait()
	close(results)
	<-semo

	rollup.average = float64(rollup.sum) / float64(rollup.count)
	fmt.Printf("done: %s\n", &rollup)
}

type flockMetrics struct {
	failed    int
	average   float64
	sum       int
	max       int
	min       int
	count     int
	sizes     stats.Float64Data
	ambiguous []error
}

func (m *flockMetrics) add(raven *ravenResult) {
	m.count += 1

	if raven.ambiguous {
		m.ambiguous = append(m.ambiguous, raven.exception)
	}

	if raven.exception != nil {
		m.failed += 1
		return
	}

	if m.max < raven.size {
		m.max = raven.size
	}

	if m.min > raven.size {
		m.min = raven.size
	}

	m.sizes = append(m.sizes, float64(raven.size))
	m.sum += raven.size
}

func (m *flockMetrics) String() string {
	quarters, e := stats.Quartile(m.sizes)

	if e != nil {
		return fmt.Sprintf("invalid flock metrics, error: %v", e)
	}

	ninenine, e := m.sizes.Percentile(99.0)

	if e != nil {
		return fmt.Sprintf("invalid flock metrics, error: %v", e)
	}

	ninefive, e := m.sizes.Percentile(95.0)

	if e != nil {
		return fmt.Sprintf("invalid flock metrics, error: %v", e)
	}

	metrics := []string{
		fmt.Sprintf("count[%d]", m.count),
		fmt.Sprintf("max[%d]", m.max),
		fmt.Sprintf("min[%d]", m.min),
		fmt.Sprintf("avg[%f]", m.average),
		fmt.Sprintf("quartiles[%v]", quarters),
		fmt.Sprintf("95[%f]", ninefive),
		fmt.Sprintf("99[%f]", ninenine),
		fmt.Sprintf("failed[%d]", m.failed),
		fmt.Sprintf("ambiguous[%d]", len(m.ambiguous)),
	}

	if len(m.ambiguous) > 0 {
		listing := make([]string, len(m.ambiguous))

		for i, e := range m.ambiguous {
			listing[i] = fmt.Sprintf("%v\n", e)
		}

		metrics = append(metrics, fmt.Sprintf("\nAMBIGUOUS RAVENS:\n%s\n", strings.Join(listing, "")))
	}

	return strings.Join(metrics, " ")
}

type resultProcessor struct {
	results chan<- *ravenResult
	queue   chan *http.Client
}

func (p *resultProcessor) fetch(resource *url.URL, group *sync.WaitGroup) {
	defer group.Done()
	client := <-p.queue
	defer func() { p.queue <- client }()

	response, e := client.Get(fmt.Sprintf("%v", resource))

	if e != nil {
		p.results <- &ravenResult{url: resource.String(), completed: true, exception: e}
		return
	}

	defer response.Body.Close()

	if response.StatusCode > 399 {
		p.results <- &ravenResult{
			url:       resource.String(),
			completed: true,
			exception: fmt.Errorf("invalid status code: %d", response.StatusCode),
		}
		return
	}

	value := response.Header.Get("Content-Length")

	if len(value) <= 0 {
		p.results <- &ravenResult{
			url:       resource.String(),
			completed: true,
			exception: fmt.Errorf("no-content-length (status code %d): %s", response.StatusCode, resource),
			ambiguous: true,
		}

		return
	}

	length, e := strconv.Atoi(value)

	if e != nil {
		p.results <- &ravenResult{
			url:       resource.String(),
			completed: true,
			exception: e,
			ambiguous: true,
		}

		return
	}

	p.results <- &ravenResult{
		url:       resource.String(),
		completed: true,
		size:      length,
		status:    response.StatusCode,
	}
}

type ravenResult struct {
	url       string
	completed bool
	status    int
	exception error
	size      int
	ambiguous bool
}

func iter(source io.Reader, max int) <-chan string {
	out := make(chan string, 100)

	go func() {
		buffered := bufio.NewReader(source)
		start := 0

		for max < 0 || start < max {
			s, e := buffered.ReadString('\n')

			if e != nil {
				break
			}

			if trimmed := strings.TrimSpace(s); !strings.HasPrefix(trimmed, "{") || !strings.HasSuffix(trimmed, "}") {
				fmt.Printf("skipping line #%d\n", start)
				continue
			}

			start = start + 1
			cleansed := strings.Trim(s, "{\" \n}")
			out <- cleansed
		}

		close(out)
	}()

	return out
}
