package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"golang.org/x/sync/errgroup"

	pb "gopkg.in/cheggaaa/pb.v2"
	elastic "gopkg.in/olivere/elastic.v5"
)

const (
	size = 10000
)

type tt int8

const (
	esToFile tt = iota
	esToEs
	fileToEs
)

var (
	logger       *log.Logger
	bar          *pb.ProgressBar
	src          string
	srcIndex     string
	srcType      string
	srcTimeField string
	srcTimeStart string
	srcTimeEnd   string
	dst          string
	dstIndex     string
	dstType      string
	transType    tt
)

func init() {
	flag.StringVar(&src, "source", "http://localhost:9200", "source, which can be an elasticsearch URL or a file path")
	flag.StringVar(&srcIndex, "source-index", "", "index to dump, applicable if source is url")
	flag.StringVar(&srcType, "source-type", "", "type of documents to dump, applicable if source is url (optional, defaults to all)")
	flag.StringVar(&srcTimeField, "source-time-field", "", "time field to query to limit the source documents to get")
	flag.StringVar(&srcTimeStart, "source-time-start", "", "start time of the time field (format: YYYY.MM.DD HH:MM:SS)")
	flag.StringVar(&srcTimeEnd, "source-time-end", "", "end time of the time field (format: YYYY.MM.DD HH:MM:SS)")
	flag.StringVar(&dst, "dest", "", "destination, which can be an elasticsearch URL or a file path (optional, defaults to stdout if not specified)")
	flag.StringVar(&dstIndex, "dest-index", "", "destination index name, applicable if dest is url (optional, defaults to the index originally dumped from)")
	flag.StringVar(&dstType, "dest-type", "", "destination type of documents, applicable if dest is url (optional, defaults to the type originally dumped from)")

}

func main() {

	// Log to stderr since stdout may be used for outputting data.
	logger = log.New(os.Stderr, "", 0)

	// Parse command line flags.
	flag.Parse()

	// Channel to pass results to.
	hits := make(chan interface{})

	g, ctx := errgroup.WithContext(context.Background())

	// Type: Transfer - es -> es, Dump - es -> file, or Load - file -> es
	if strings.HasPrefix(src, "http") && strings.HasPrefix(dst, "http") {
		transType = esToEs
	} else if strings.HasPrefix(src, "http") {
		transType = esToFile
	} else if strings.HasPrefix(dst, "http") {
		transType = fileToEs
	}

	// Check that required parameters are set.
	if transType == esToEs || transType == esToFile {
		if srcIndex == "" {
			logger.Fatal("please specify a source index")
		}
	} else {
		if dst == "" {
			logger.Fatal("please specify an elastic destination url")
		}
	}

	if transType == esToEs || transType == esToFile {
		client, total, err := setupElasticSource(src, srcIndex, srcType)
		if err != nil {
			logger.Fatal(err)
		}
		bar = pb.New64(total).SetWriter(os.Stderr).Start()

		readDataFromElastic(ctx, g, client, hits)

		// Write output to file or elastic.
		if transType == esToFile {
			err = writeDataToFile(ctx, g, dst, bar, hits)
			if err != nil {
				logger.Fatal(err)
			}
		} else {
			// TODO: write output to Elasticsearch
		}
	} else {
		// Elasticsearch client.
		client, err := elastic.NewClient(elastic.SetURL(dst))
		if err != nil {
			logger.Fatalf("error creating elastic client to url %s: %s", dst, err.Error())
		}

		total, err := lineCount(src)
		if err != nil {
			logger.Fatal(err)
		}
		bar = pb.New64(total).SetWriter(os.Stderr).Start()

		err = readDataFromFile(ctx, g, src, hits)
		if err != nil {
			logger.Fatal(err)
		}

		err = writeDataToElastic(ctx, g, client, dstIndex, dstType, bar, hits)
		if err != nil {
			logger.Fatal(err)
		}

	}

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		logger.Fatal(err)
	}

	// Done.
	bar.Finish()

}

// setupElasticSource configures the elastic client and returns the client
// and the total number of documents in the index.
func setupElasticSource(url, index, typ string) (*elastic.Client, int64, error) {
	client, err := elastic.NewClient(elastic.SetURL(url))
	if err != nil {
		return nil, 0, fmt.Errorf("error creating elastic client to url %s: %s", url, err.Error())
	}

	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		return nil, 0, fmt.Errorf("error checking if index %s exists: %s", index, err.Error())
	}
	if !exists {
		return nil, 0, fmt.Errorf("index %s does not exist - you can only export an existing index", index)
	}

	counter := client.Count(index).Type(typ)
	var total int64
	if srcTimeField != "" {
		q := elastic.NewRangeQuery(srcTimeField).Format("yyyy.MM.dd HH:mm:ss").Gt(srcTimeStart).Lte(srcTimeEnd)
		total, err = counter.Query(q).Do(context.Background())
	} else {
		total, err = counter.Do(context.Background())
	}
	if err != nil {
		return nil, 0, fmt.Errorf("error counting documents in index %s: %s", index, err.Error())
	}
	return client, total, nil
}

func readDataFromElastic(ctx context.Context, g *errgroup.Group, client *elastic.Client, hits chan interface{}) {
	g.Go(func() error {
		defer close(hits)

		scroll := client.Scroll(srcIndex).Size(size)

		// Set up query to limit data if set.
		if srcTimeField != "" {
			q := elastic.NewRangeQuery(srcTimeField).Format("yyyy.MM.dd HH:mm:ss").Gt(srcTimeStart).Lte(srcTimeEnd)
			scroll.Query(q)
		}

		for {
			results, err := scroll.Do(context.Background())
			if err == io.EOF {
				return nil // all results retrieved
			}
			if err != nil {
				return err // something went wrong
			}
			// Send the hits to the hits channel
			for _, hit := range results.Hits.Hits {
				select {
				case hits <- *hit:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
	})
}

func readDataFromFile(ctx context.Context, g *errgroup.Group, filePath string, hits chan interface{}) error {
	var in *os.File
	var err error
	var gzw *gzip.Reader
	var r *bufio.Reader

	doGzip := strings.HasSuffix(filePath, ".gz")

	if filePath != "" {
		in, err = os.Open(filePath)
		if err != nil {
			return fmt.Errorf("unable to create destination file %s: %s", filePath, err.Error())
		}
		if doGzip {
			gzw, err = gzip.NewReader(in)
			if err != nil {
				return err
			}
			r = bufio.NewReader(gzw)
		} else {
			r = bufio.NewReader(in)
		}
	} else {
		r = bufio.NewReader(os.Stdin)
	}

	g.Go(func() error {
		var line []byte
		for {
			line, err = r.ReadBytes('\n')
			if err == io.EOF {
				gzw.Close()
				in.Close()
				close(hits)
				return nil
			}
			if err != nil {
				return err
			}
			hits <- line
		}
	})
	return nil
}

func writeDataToElastic(ctx context.Context, g *errgroup.Group, client *elastic.Client, dstIndex, dstType string, bar *pb.ProgressBar, hits chan interface{}) error {
	bulk, err := client.BulkProcessor().Name("bulker").Workers(2).Do(context.Background())
	if err != nil {
		return err
	}

	g.Go(func() error {
		for h := range hits {
			hit := h.([]byte)
			var res elastic.SearchHit
			err = json.Unmarshal(hit, &res)
			if err != nil {
				logger.Printf("error unmarshaling json: %s", err)
			}

			i := dstIndex
			if dstIndex == "" {
				i = res.Index
			}
			t := dstType
			if dstType == "" {
				t = res.Type
			}
			r := elastic.NewBulkIndexRequest().Index(i).Type(t).Id(res.Id).Doc(*res.Source)
			bulk.Add(r)

			bar.Increment()

			// Terminate early?
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		bulk.Flush()
		bulk.Close()
		return nil
	})
	return nil
}

func writeDataToFile(ctx context.Context, g *errgroup.Group, filePath string, bar *pb.ProgressBar, hits chan interface{}) error {
	var out *os.File
	var err error
	var gzw *gzip.Writer
	var w *bufio.Writer
	if filePath != "" {
		out, err = os.Create(filePath)
		if err != nil {
			return fmt.Errorf("unable to create destination file %s: %s", filePath, err.Error())
		}
	} else {
		out = os.Stdout
	}

	doGzip := strings.HasSuffix(filePath, ".gz")

	g.Go(func() error {
		if doGzip {
			gzw = gzip.NewWriter(out)
			w = bufio.NewWriter(gzw)
		} else {
			w = bufio.NewWriter(out)
		}
		for h := range hits {
			b, err := json.Marshal(h.(elastic.SearchHit))
			if err != nil {
				logger.Printf("error marshaling json: %s", err)
			}

			w.Write(b)
			w.Write([]byte("\n"))

			bar.Increment()

			// Terminate early?
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		w.Flush()
		if doGzip {
			gzw.Close()
		}
		out.Close()
		return nil
	})
	return nil
}

// lineCount will return the number of lines in a given file.
func lineCount(filename string) (int64, error) {
	lc := int64(0)
	f, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		lc++
	}
	return lc, s.Err()
}
