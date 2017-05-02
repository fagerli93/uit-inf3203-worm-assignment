package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"time"
	"strconv"
	"sync"

)

var maxRunTime time.Duration

var wormgatePort string
var segmentPort string

var hostname string

//host name with out .local at the end
var hostname2 string

var targetSegments int32


var reachablehosts []string

var activehosts []string

var oldactivehosts []string

var SLEEPTIME = 1
var killrate int




type hoststatus struct {
    addr string
    status int
}



func main() {

	hostname, _ = os.Hostname()
	log.SetPrefix(hostname + " segment: ")
	segmentClient = createClient()
	var spreadMode = flag.NewFlagSet("spread", flag.ExitOnError)
	addCommonFlags(spreadMode)
	var spreadHost = spreadMode.String("host", "localhost", "host to spread to")

	var runMode = flag.NewFlagSet("run", flag.ExitOnError)
	addCommonFlags(runMode)

	if len(os.Args) == 1 {
		log.Fatalf("No mode specified\n")
	}

	switch os.Args[1] {
	case "spread":
		spreadMode.Parse(os.Args[2:])
		//logger("test")
		sendSegment(*spreadHost)
	case "run":
		runMode.Parse(os.Args[2:])
		//hostname2 = strings.Trim(hostname, ".local")
		hostname2 = hostname[:len(hostname)-6]

		activehosts = append(activehosts, hostname2)
		startSegmentServer()

	default:
		log.Fatalf("Unknown mode %q\n", os.Args[1])
	}
}

func addCommonFlags(flagset *flag.FlagSet) {
	flagset.StringVar(&wormgatePort, "wp", ":8181", "wormgate port (prefix with colon)")
	flagset.StringVar(&segmentPort, "sp", ":8182", "segment port (prefix with colon)")
	flagset.DurationVar(&maxRunTime, "maxrun", time.Minute*10, "max time to run (in case you forget to shut down)")
}

func logger(msg string) {
	var filename = "logs.txt"
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	defer file.Close()
	fmt.Fprintf(file, msg+"\n")
	check(err)
}

func check(e error) {
	if e != nil {
			panic(e)
	}
}

func createClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{},
	}
}
var segmentClient *http.Client



func updateTargetSegments(newts int32, node string) error {
	url := fmt.Sprintf("http://%s%s/updatetargetsegment", node, segmentPort)
	postBody := strings.NewReader(fmt.Sprint(newts))
	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting updatetargetSegments %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err
}
func contains(s []string, e string) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}
func findFreeNodeAddress()string{
	for i := range reachablehosts{
		if reachablehosts[i] != hostname2{
			if contains(activehosts, reachablehosts[i]) == false {
				//log.Printf("activehosts: "+fmt.Sprint(activehosts)+" reachablehosts[i]: "+reachablehosts[i])
				return reachablehosts[i]
			}
		}
	}
	return "-NO MORE FREE DDRESSES-"
}
func findTakenNodeAddress()string{
	for i := range activehosts{
		if reachablehosts[i] != hostname2 {
			if contains(activehosts, reachablehosts[i]) == true {
				return reachablehosts[i]
			}
		}
	}
	return "-NO MORE TAKEN ADDRESSES-"
}







func sendSegment(address string) int{
	//log.Printf("iam here in start sendSegment now")
	url := fmt.Sprintf("http://%s%s/wormgate?sp=%s", address, wormgatePort, segmentPort)
	filename := "tmp.tar.gz"

	log.Printf("Spreading to %s", url)

	// ship the binary and the qml file that describes our screen output
	tarCmd := exec.Command("tar", "-zc", "-f", filename, "segment")
	tarCmd.Run()
	defer os.Remove(filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Panic("Could not read input file", err)
	}

	resp, err := http.Post(url, "string", file)
	if err != nil {
		log.Panic("POST error ", err)
	}

	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode == 200 {
		log.Println("Received OK from server")
	} else {
		log.Println("Response: ", resp)
	}
	return resp.StatusCode
}

func startSegmentServer() {
	//log.Printf("test")
	var ts int32
	ts = 1
	killrate = 0
	atomic.StoreInt32(&targetSegments, ts)
	reachablehosts = append(reachablehosts, fetchReachableHosts()...)
	findTs()
	log.Printf("TS: "+fmt.Sprint(atomic.LoadInt32(&targetSegments)))

	//log.Printf("iam here in start startSegmentServer now")

	// Quit if maxRunTime timeout
	exitReason := make(chan string, 1)
	go func() {
		time.Sleep(maxRunTime)
		exitReason <- fmt.Sprintf("maxrun timeout: %s", maxRunTime)
	}()
	go func() {
		reason := <-exitReason
		log.Printf(reason)
		log.Print("Shutting down")
		os.Exit(0)
	}()

	http.HandleFunc("/", IndexHandler)
	http.HandleFunc("/targetsegments", targetSegmentsHandler)
	http.HandleFunc("/ping", pingHandler)
	http.HandleFunc("/shutdown", shutdownHandler)
	http.HandleFunc("/shutdown2", shutdownHandler2)
	http.HandleFunc("/updatetargetsegment", updateTargetSegmentsHandler)
	http.HandleFunc("/findts", findTsHandler)


	log.Printf("Starting segment server on %s%s\n", hostname, segmentPort)






	//log.Printf("ALSO HERE")
	go checkAll()

	//log.Printf("YOUOHJDS")
	err := http.ListenAndServe(segmentPort, nil)
	if err != nil {
		log.Panic(err)
	}
	//log.Printf("CHECKING: ")
	//checkAll()
}




func IndexHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the request body. But we should consume it anyway.
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	//killRateGuess := 2.0
	killRateGuess := float32(killrate)

	fmt.Fprintf(w, "%.3f\n", killRateGuess)
}
func pingHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the request body. But we should consume it anyway.
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	killRateGuess := 2.0

	fmt.Fprintf(w, "%.3f\n", killRateGuess)
}

func updateTargetSegmentsHandler(w http.ResponseWriter, r *http.Request) {
	//log.Printf("IN UPDATETARGETSEGMENTHANDLER")
	var ts int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ts)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
	}
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	atomic.StoreInt32(&targetSegments, ts)
}

func targetSegmentsHandler(w http.ResponseWriter, r *http.Request) {

	var ts int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ts)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
	}

	// Consume and close rest of body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	atomic.StoreInt32(&targetSegments, ts)
	//log.Println("BEFORE SPREAD")
	spreadTs(ts)
	//log.Println("AFTER SPREAD")

	//log.Println("targetSegment: %d", atomic.LoadInt32(&targetSegments))
}

func spreadTs(ts int32){
	for i :=range activehosts{
		if activehosts[i] != hostname2 {
			//log.Println("spreading ts to: "+activehosts[i])
			updateTargetSegments(ts, activehosts[i])
		}
	}
}

func remove(s []string, r string) []string {
    for i, v := range s {
        if v == r {
            return append(s[:i], s[i+1:]...)
        }
    }
    return s
}


func shutdownHandler(w http.ResponseWriter, r *http.Request) {

	// Consume and close body
	var wg sync.WaitGroup
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()
	for i:= range activehosts {
		if activehosts[i] != hostname2 {
			wg.Add(1)
			go doAllWormShutdownPost(activehosts[i], &wg)
		}
	}

	// Shut down
	wg.Wait()
	log.Printf("Received shutdown command, committing suicide")
	os.Exit(0)
}
func shutdownHandler2(w http.ResponseWriter, r *http.Request) {

	// Consume and close body
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	// Shut down
	log.Printf("Received shutdown command, committing suicide")
	os.Exit(0)
}

func fetchReachableHosts() []string {
	url := fmt.Sprintf("http://localhost%s/reachablehosts", wormgatePort)
	resp, err := http.Get(url)
	if err != nil {
		return []string{}
	}

	var bytes []byte
	bytes, err = ioutil.ReadAll(resp.Body)
	body := string(bytes)
	resp.Body.Close()

	trimmed := strings.TrimSpace(body)
	nodes := strings.Split(trimmed, "\n")
	nodes = remove(nodes, "compute-1-4")
	nodes = remove(nodes, "compute-2-20")

	//tmp_nodes := []string{"compute-1-0", "compute-1-1", "compute-1-2", "compute-1-3"}

	return nodes
}


func httpGetOk(client *http.Client, url string) (bool, string, error) {
	resp, err := client.Get(url)
	isOk := err == nil && resp.StatusCode == 200
	body := ""
	if err != nil {
		if strings.Contains(fmt.Sprint(err), "connection refused") {
			// ignore connection refused errors
			err = nil
		} else {
			log.Printf("Error pinging %s: %s", url, err)
		}
	} else {
		var bytes []byte
		bytes, err = ioutil.ReadAll(resp.Body)
		body = string(bytes)
		resp.Body.Close()
	}
	return isOk, body, err
}

func ping(reachablehost string, channel chan hoststatus, wg *sync.WaitGroup, i int){

	var status bool
	url := fmt.Sprintf("http://%s%s/ping", reachablehost, segmentPort)


	status, _, _ = httpGetOk(segmentClient, url)

	host := new(hoststatus)
	host.addr = reachablehost

	if status == true {

		host.status = 1

	}else{

		host.status = 0

	}
	channel <- *host
	//log.Println("host addr["+strconv.Itoa(i)+"]: "+host.addr+" host status: "+strconv.Itoa(host.status))
	wg.Done()
}

func checkAll(){

	var wg sync.WaitGroup
	for{
		reachablehosts2 = fetchReachableHosts()
		oldactivehosts = activehosts
		c := make(chan hoststatus, len(reachablehosts2)-1)
		for i:= range reachablehosts2 {
			if reachablehosts2[i] != hostname2{
				wg.Add(1)
				go ping(reachablehosts2[i], c, &wg, i)
			}
		}


		//log.Printf("+++++++++++++++++++++++++++++++++++")
		wg.Wait()
		close(c)
		updateActiveHostList1(c)

		AddOrRemoveSegments()
		calcKillRate()
		//log.Printf("-----------------------------------")

		//wg1.Wait()
	}
}
func updateActiveHostList1(channel chan hoststatus){
	//defer wg1.Done()

	//log.Printf("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

	for  host := range channel {

		if host.status == 1 {
			//log.Printf("ADD2: host.add: "+host.addr)
			if contains(activehosts, host.addr) == false {
				//log.Printf("ADD: host.add: "+host.addr)
				activehosts = append(activehosts, host.addr)
			}
		}else{

			if contains(activehosts, host.addr) == true{

				//log.Printf("noting to remove["+strconv.Itoa(i)+"]")
				//log.Printf("REMOVE: host.add: "+host.addr)
				activehosts = remove(activehosts, host.addr)
			}
		}


	}

}

func AddOrRemoveSegments(){
	ts := atomic.LoadInt32(&targetSegments)

	numberOfActiveSegments := len(activehosts)

	//log.Printf("numberOfActiveSegments: "+strconv.Itoa(numberOfActiveSegments)+" ts: "+fmt.Sprint(ts)+" list: "+fmt.Sprint(activehosts))
	if numberOfActiveSegments < int(ts) {
		//log.Printf("+++++++++++++++++++")
		host := findFreeNodeAddress()
		if host != "-NO MORE FREE DDRESSES-" {
			sendSegment(host)
			updateTargetSegments(ts, host)
			activehosts = append(activehosts, host)

			//log.Printf(fmt.Sprint(activehosts))
			//log.Printf("+++++++++++++++++++")
		}
	}

	if numberOfActiveSegments > int(ts) {
		host := findTakenNodeAddress()
		if host != "-NO MORE TAKEN ADDRESSES-" {
			doWormShutdownPost(host)
			//activehosts = remove(activehosts, host)
		}
	}
	time.Sleep(200 * time.Millisecond)
}


func doWormShutdownPost(node string) error {
	log.Printf("Posting shutdown to %s", node)

	url := fmt.Sprintf("http://%s%s/shutdown2", node, segmentPort)

	resp, err := segmentClient.PostForm(url, nil)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting targetSegments %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err
}

func doAllWormShutdownPost(node string, wg *sync.WaitGroup) error {
	log.Printf("Posting shutdown to %s", node)

	url := fmt.Sprintf("http://%s%s/shutdown", node, segmentPort)

	resp, err := segmentClient.PostForm(url, nil)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting targetSegments %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	wg.Done()
	return err
}

func findTs(){
	//log.Println("in FINDTS")
	for i:= range reachablehosts {
		if reachablehosts[i] != hostname2{
			//log.Println("and here")

				var status bool
				url := fmt.Sprintf("http://%s%s/findts", reachablehosts[i], segmentPort)
				status, ts, _ := httpGetOk(segmentClient, url)
				//log.Println("findTS: "+strconv.FormatBool(status)+" ts: "+ts+" checking"+reachablehosts[i])


				if status == true{
					conv_ts, _ := strconv.ParseInt(ts,10,32)
					//log.Println("-------------------------")
					//ts1 := strings.TrimRight(ts, "\n")
					//log.Println(strconv.Atoi(ts))
					//log.Println("-------------------------")
					atomic.StoreInt32(&targetSegments, int32(conv_ts))
					//log.Println("findTS: "+strconv.FormatBool(status)+" ts_string: "+ts+" checking"+reachablehosts[i]+ " atomic: "+strconv.Itoa(int(targetSegments)))
					break
				}
		}
	}
}


func findTsHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the request body. But we should consume it anyway.
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	ts := atomic.LoadInt32(&targetSegments)

	//fmt.Fprintf(w, fmt.Sprintf(ts))
	//log.Println("handleTS: "+fmt.Sprint(ts))
	fmt.Fprintf(w, "%d", ts)
}

func calcKillRate(){
	old := len(oldactivehosts)
	new := len(activehosts)



	if new < old{
		killrate = old - new
		log.Printf("---------------------")

	}else{
		killrate = 0
	}
}
