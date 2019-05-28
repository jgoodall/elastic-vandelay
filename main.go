package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/schollz/progressbar/v2"
	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
	"gopkg.in/alecthomas/kingpin.v2"
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
	app   = kingpin.New("elastic-vandelay", "A tool to import and export an elasticsearch index")
	debug = app.Flag("debug", "Enable debug mode").Bool()

	// Export from es to a file
	exportCmd       = app.Command("export", "Export an index to a file")
	exportSrcURL    = exportCmd.Flag("source-url", "Elasticsearch host to export (http://host:port/)").Required().URL()
	exportSrcIndex  = exportCmd.Flag("source-index", "Elasticsearch index to export (http://host:port/)").Required().String()
	exportDstFile   = exportCmd.Flag("dest-file", "File path to save the export to (use '.gz' suffix to gzip the data)").Required().OpenFile(os.O_CREATE|os.O_EXCL, 0644)
	exportTimeField = exportCmd.Flag("time-field", "Elasticsearch time field to filter data on").String()
	exportTimeStart = exportCmd.Flag("time-start", "The start time value to use to filter the data to export (format: YYYY.MM.DD HH:MM:SS)").String()
	exportTimeEnd   = exportCmd.Flag("time-end", "The end time value to use to filter the data to export (format: YYYY.MM.DD HH:MM:SS)").String()

	// Import from file to es
	importCmd      = app.Command("import", "Import an index")
	importSrcFile  = importCmd.Flag("source-file", "File path of the exported index to import (a file with '.gz' suffix will be gunzipped first)").Required().File()
	importDstURL   = importCmd.Flag("dest-url", "Elasticsearch host to import the index to (http://host:port/)").Required().URL()
	importDstIndex = importCmd.Flag("dest-index", "Elasticsearch index to import").Required().String()
)

var (
	logger *log.Logger
	bar    *progressbar.ProgressBar
)

func main() {
	logger = log.New(os.Stderr, "", 0)
	switch kingpin.MustParse(app.Parse(os.Args[1:])) {
	case exportCmd.FullCommand():
		kingpin.FatalIfError(doExport(), "Export failed")
	case importCmd.FullCommand():
		kingpin.FatalIfError(doImport(), "Import failed")
	}
}

func doExport() error {
	logger.Printf("exporting from index %s to file %s\n", *exportSrcURL, (*exportDstFile).Name())
	client, total, err := connectElasticSource((*exportSrcURL).String(), *exportSrcIndex)
	if err != nil {
		return err
	}
	// Channel to pass data results to.
	hits := make(chan interface{})
	g, ctx := errgroup.WithContext(context.Background())
	startTime := time.Now()
	bar = progressbar.NewOptions64(total, progressbar.OptionSetRenderBlankState(true), progressbar.OptionSetWriter(os.Stderr))

	readDataFromElastic(ctx, *exportSrcIndex, *exportTimeField, *exportTimeStart, *exportTimeEnd, g, client, hits)
	mappings, err := readMappingsFromElastic(client, *exportSrcIndex)
	if err != nil {
		logger.Fatal(err)
	}
	err = writeMappingsToFile((*exportDstFile).Name(), mappings)
	if err != nil {
		logger.Fatal(err)
	}
	err = writeDataToFile(ctx, g, (*exportDstFile).Name(), hits)
	if err != nil {
		logger.Fatal(err)
	}

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		logger.Fatal(err)
	}
	bar.Finish()
	logger.Printf("\nexport completed in %s\n", time.Now().Sub(startTime).String())

	return nil
}

func doImport() error {
	logger.Printf("importing from file %s to index %s\n", (*importSrcFile).Name(), *importDstURL)
	client, err := connectElasticDest((*importDstURL).String(), *importDstIndex)
	if err != nil {
		return err
	}
	// Channel to pass data results to.
	hits := make(chan interface{})
	g, ctx := errgroup.WithContext(context.Background())
	startTime := time.Now()
	fileStat, err := (*importSrcFile).Stat()
	if err != nil {
		return err
	}
	bar = progressbar.NewOptions64(fileStat.Size(), progressbar.OptionSetRenderBlankState(true), progressbar.OptionSetWriter(os.Stderr))

	mappings, err := readMappingsFromFile((*importSrcFile).Name())
	if err != nil {
		logger.Fatal(err)
	}
	err = writeMappingsAsStringToElastic(client, (*importDstURL).String(), *importDstIndex, string(mappings))
	if err != nil {
		logger.Fatal(err)
	}
	err = readDataFromFile(ctx, g, (*importSrcFile).Name(), hits)
	if err != nil {
		logger.Fatal(err)
	}
	err = writeDataToElastic(ctx, g, client, *importDstIndex, hits)
	if err != nil {
		logger.Fatal(err)
	}

	// Check whether any goroutines failed.
	if err := g.Wait(); err != nil {
		logger.Fatal(err)
	}
	bar.Finish()
	logger.Printf("\nimport completed in %s\n", time.Now().Sub(startTime).String())

	return nil
}

// connectElasticSource configures the elastic client and returns the client
// and the total number of documents in the index.
func connectElasticSource(url, index string) (*elastic.Client, int64, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
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

	counter := client.Count(index)
	var total int64
	if *exportTimeField != "" {
		q := elastic.NewRangeQuery(*exportTimeField).Format("yyyy.MM.dd HH:mm:ss").Gt(*exportTimeStart).Lte(*exportTimeEnd)
		total, err = counter.Query(q).Do(context.Background())
	} else {
		total, err = counter.Do(context.Background())
	}
	if err != nil {
		return nil, 0, fmt.Errorf("error counting documents in index %s: %s", index, err.Error())
	}
	return client, total, nil
}

// connectElasticDest configures the elastic client and returns the client
// and the total number of documents in the index.
func connectElasticDest(url, index string) (*elastic.Client, error) {
	client, err := elastic.NewClient(
		elastic.SetURL(url),
		elastic.SetHealthcheck(false),
		elastic.SetSniff(false),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating elastic client to url %s: %s", url, err.Error())
	}

	exists, err := client.IndexExists(index).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("error checking if index %s exists: %s", index, err.Error())
	}
	if exists {
		return nil, fmt.Errorf("index %s exists - you can only import to a new index", index)
	}
	return client, nil
}

// readDataFromElastic reads data from elasticsearch and sends each result
// to the channel.
func readDataFromElastic(ctx context.Context, srcIndex, srcTimeField, srcTimeStart, srcTimeEnd string, g *errgroup.Group, client *elastic.Client, hits chan interface{}) {
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

// readDataFromFile reads data from a file and sends each line to the
// channel.
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
			r = bufio.NewReaderSize(gzw, 16384)
		} else {
			r = bufio.NewReaderSize(in, 16384)
		}
	} else {
		r = bufio.NewReader(os.Stdin)
	}

	g.Go(func() error {
		var line []byte
		for {
			line, err = r.ReadBytes('\n')
			if err == io.EOF {
				if doGzip {
					gzw.Close()
				}
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

// writeDataToElastic uses the bulk processor to send bulk requests to
// Elasticsearch for each document sent on channel.
func writeDataToElastic(ctx context.Context, g *errgroup.Group, client *elastic.Client, dstIndex string, hits chan interface{}) error {
	w := runtime.NumCPU()
	bulk, err := client.BulkProcessor().Name("bulker").Workers(w).Do(context.Background())
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
			r := elastic.NewBulkIndexRequest().Index(i).Id(res.Id).Doc(res.Source)
			bulk.Add(r)

			bar.Add64(int64(len(hit)))

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

// writeDataToFile writes each document sent on channel to a file.
func writeDataToFile(ctx context.Context, g *errgroup.Group, filePath string, hits chan interface{}) error {
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

			bar.Add64(1)

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
	buf := make([]byte, 32*1024)
	count := int64(0)
	lineSep := []byte{'\n'}
	r, err := os.Open(filename)
	if err != nil {
		return 0, err
	}
	for {
		c, err := r.Read(buf)
		count += int64(bytes.Count(buf[:c], lineSep))

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

// readMappingsFromElastic gets the mappings for a given index.
func readMappingsFromElastic(client *elastic.Client, index string) (m map[string]interface{}, err error) {
	m, err = client.GetMapping().Index(index).Do(context.Background())
	return
}

// readMappingsFromFile gets the mappings from a json file.
func readMappingsFromFile(file string) (m []byte, err error) {
	baseFileName := strings.Replace(strings.Replace(file, ".gz", "", 1), ".json", "", 1)
	f := baseFileName + "-mapping.json"
	if _, e := os.Stat(f); os.IsNotExist(e) {
		return nil, fmt.Errorf("mappings file does not exist: %s", f)
	}
	r, err := os.Open(f)
	if err == nil {
		m, err = ioutil.ReadAll(r)
	}
	defer r.Close()
	return
}

// writeMappingsAsMapToElastic sends mappings to elasticsearch.
func writeMappingsAsMapToElastic(client *elastic.Client, index string, m map[string]interface{}) (err error) {
	_, err = client.PutMapping().BodyJson(m).Index(index).Do(context.Background())
	return
}

// writeMappingsAsStringToElastic sends mappings to elasticsearch.
func writeMappingsAsStringToElastic(client *elastic.Client, dstURL, index, m string) (err error) {
	// Fail if the index already exists.
	exists, _ := client.IndexExists(index).Do(context.Background())
	if exists {
		err = fmt.Errorf("index %s already exists, if you want to replace it delete it first - 'curl -XDELETE %s/%s'", index, dstURL, index)
		return err
	}

	// Parse string into map. Top level of map is old index name.
	mappings, ok := gjson.Parse(m).Value().(map[string]interface{})
	if !ok {
		return fmt.Errorf("unable to parse json mappings")
	}

	// There should only be one top level object - the old index name - so
	// we are just getting the value of that key.
	var tm map[string]interface{}
	for _, v := range mappings {
		tm = v.(map[string]interface{})
		break
	}
	typeMappings := tm["mappings"]

	// The new map.
	newMap := map[string]interface{}{
		"mappings": map[string]interface{}{},
	}

	// Loop through each type and add it to the mapping.
	for key, val := range typeMappings.(map[string]interface{}) {
		newMap["mappings"].(map[string]interface{})[key] = val
	}

	// Create the new index with the mappings.
	_, err = client.CreateIndex(index).BodyJson(newMap).Do(context.Background())
	if err != nil {
		return
	}

	return
}

// writeMappingsToFile writes JSON of mappings to a file.
func writeMappingsToFile(file string, m map[string]interface{}) (err error) {
	// Strip extension, output.json becomes output-mapping.json
	f := strings.TrimSuffix(file, ".gz")
	f = strings.TrimSuffix(f, ".json")
	f = f + "-mapping.json"
	var mapJSON []byte
	mapJSON, err = json.Marshal(m)
	if err == nil {
		err = ioutil.WriteFile(f, mapJSON, 0644)
	}
	return
}
