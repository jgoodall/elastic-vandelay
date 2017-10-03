package main

import (
	"bufio"
	"context"
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

var (
	src      string
	srcIndex string
	typ      string
	dst      string
	verbose  bool
)

func init() {
	flag.StringVar(&src, "source", "http://localhost:9200", "source, which can be an elasticsearch URL or a file path")
	flag.StringVar(&srcIndex, "source-index", "", "index to dump (if source is url)")
	flag.StringVar(&typ, "typ", "", "type of documents to dump (optional, defaults to all)")
	flag.StringVar(&dst, "dest", "", "destination, which can be an elasticsearch URL or a file path (optional, defaults to stdout if not specified)")
	flag.BoolVar(&verbose, "verbose", false, "print progress to stderr")
}

func main() {

	logger := log.New(os.Stderr, "", 0)

	flag.Parse()

	// source is elasticsearch url.
	if strings.HasPrefix(src, "http") {
		if srcIndex == "" {
			logger.Fatal("please specify an index")
		}
		client, total, err := setupElasticClient(src, srcIndex)
		if err != nil {
			logger.Fatal(err)
		}

		if verbose {
			logger.Printf("total documents in index %s to dump: %d\n", srcIndex, total)
		}

		// Set up progress bar.
		bar := pb.New64(total)
		bar.SetWriter(os.Stderr)
		bar.Start()

		// Channel to pass results to.
		hits := make(chan elastic.SearchHit)

		g, ctx := errgroup.WithContext(context.Background())

		// First goroutine will get the data.
		g.Go(func() error {
			defer close(hits)

			scroll := client.Scroll(srcIndex).Size(size)
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

		// 2nd goroutine receives hits and deserializes them.
		var out *os.File
		var w *bufio.Writer
		if dst != "" {
			out, err = os.Create(dst)
			if err != nil {
				logger.Fatalf("unable to create destination file %s: %s", dst, err)
			}
		} else {
			out = os.Stdout
		}

		g.Go(func() error {
			w = bufio.NewWriter(out)
			for hit := range hits {
				line := fmt.Sprintf(`{"_index": "%s", "_type": "%s", "_id": "%s", "_source": "%s"}`, hit.Index, hit.Type, hit.Id, string(*hit.Source))

				_, err := w.WriteString(line + "\n")
				if err != nil {
					logger.Printf("write error %s\n", err)
				}

				bar.Increment()

				// Terminate early?
				select {
				default:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			w.Flush()
			out.Close()
			return nil
		})

		// Check whether any goroutines failed.
		if err := g.Wait(); err != nil {
			log.Fatal(err)
		}

		// Done.
		bar.Finish()

	}

}

// setupElasticClient configures the elastic client and returns the client
// and the total number of documents in the index.
func setupElasticClient(url, index string) (*elastic.Client, int64, error) {
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

	total, err := client.Count(index).Type(typ).Do(context.Background())
	if err != nil {
		return nil, 0, fmt.Errorf("error counting documents in index %s: %s", index, err.Error())
	}
	return client, total, nil
}
