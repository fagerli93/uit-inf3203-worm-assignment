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

var SLEEPTIME = 1




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
/*
func CheckReachableHostsForSegments() {
	for _, element := range reachablehosts {
		if(elemtent != hostname){
			//PING
			http.Get(element)
			//if(PING SUCCESS)
			//	ADD TO LIST OF ACTIVE SEGMENTS
		}
	}
}*/
/*
func updateActiveSegmentsWithNewInfo(newts int32, newnode string){

	for i:= range activehosts {
		if(activehosts[i] != hostname2 && activehosts[i] != newnode){
			updateTargetSegments(newts, activehosts[i])
			updateActiveHostListRemote(newnode, activehosts[i])
		}
	}
}
*/
func createClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{},
	}
}
var segmentClient *http.Client

//SENDS FROM THE ONE NODE TO ANOTHER NODE THE ACTIVE HOST LIST
func updateActiveHostListRemote(newnode string, node string) error{
	url := fmt.Sprintf("http://%s%s/updateactivehostlist", node, segmentPort)
	postBody := strings.NewReader(fmt.Sprint(newnode))
	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting targetSegments %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err

}

func updateTargetSegments(newts int32, node string) error {
	url := fmt.Sprintf("http://%s%s/updatetargetsegment", node, segmentPort)
	postBody := strings.NewReader(fmt.Sprint(newts))
	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting targetSegments %s: %s", node, err)
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
				return reachablehosts[i]
			}
		}
	}
	return "-NO MORE FREE DDRESSES-"
}
/*
func findFreeNodeAddress()string{
	for i := range reachablehosts{
		for j := range activehosts{
			if(reachablehosts[i] != hostname2) && (reachablehosts[i] != activehosts[j]){
				log.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
				log.Println("reachablehosts: "+reachablehosts[i]+" hostname: "+hostname2+" activehost: "+activehosts[j])
				log.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
				return reachablehosts[i]
			}
		}
	}
	return "ERROR"
}*/
/*
func addMoreSegments(newTargetSegments int32){
	Loop:
	for targetSegments < newTargetSegments {

		for i := range reachablehosts {
			if(reachablehosts[i] != hostname){
				resp := sendSegment(reachablehosts[i])
				if(resp == 200){
					updateTargetSegments(newTargetSegments, reachablehosts[i])
					//updateExistingSegmentsWithNewInfo(newTargetSegments, rea)

					break Loop
				}
			}
		}
	}
}

*/
func update(newTargetSegments int32, new_node string){
	//sends to ts to the newly created segment
	atomic.StoreInt32(&targetSegments, newTargetSegments)
	activehosts = append(activehosts, new_node)
	updateTargetSegments(newTargetSegments, new_node)
	time.Sleep(time.Second * 1)
	test := updateActiveHostListNewNode(new_node)
	log.Println("HHHHHHHHHHHHHHHHHHHHHHHHHHH")
	log.Println(test)
	log.Println("HHHHHHHHHHHHHHHHHHHHHHHHHHH")

	//update all other active nodes with ts and new active segment
	for i :=range activehosts{
		if activehosts[i] != hostname2 && activehosts[i] == new_node{
			updateTargetSegments(newTargetSegments, activehosts[i])
			updateActiveHostListRemote(new_node, activehosts[i])
		}

	}
}
//UPDATES THE NEW NODES WE CREATED WITH ALL ACTIVE HOSTS
func updateActiveHostListNewNode(node string) error{

	url := fmt.Sprintf("http://%s%s/updateactivehostlistnewnode", node, segmentPort)
	log.Println("++++++++++++++++++++++++++++++++++++++++++++")
	stringActiveHosts :=strings.Join(activehosts, " ")
	log.Println("stringActiveHosts: '"+stringActiveHosts+"'")
	log.Println("++++++++++++++++++++++++++++++++++++++++++++")

	postBody := strings.NewReader(fmt.Sprint(stringActiveHosts))
	resp, err := segmentClient.Post(url, "text/plain", postBody)
	if err != nil && !strings.Contains(fmt.Sprint(err), "refused") {
		log.Printf("Error posting updateActiveHostLIstNewNOde %s: %s", node, err)
	}
	if err == nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
	return err


}


func addMoreSegments(newTargetSegments int32){
	ts := atomic.LoadInt32(&targetSegments)
	var new_node string
	for ts < newTargetSegments{
		new_node = findFreeNodeAddress()
		//log.Println("++++++++++++++++++++++++++++++++")
		//log.Println("new_node: "+new_node)
		//log.Println("active_host:"+fmt.Sprint(activehosts))

		//log.Println("++++++++++++++++++++++++++++++++")
		resp := sendSegment(new_node)
		if(resp == 200){
			log.Println("--------------------------------")
			log.Println("targetSegments: "+fmt.Sprint(ts)+" -- new_node: "+new_node+" --active:hosts:'"+activehosts[0]+"'")
			log.Println("--------------------------------")
			update(ts+1, new_node)
			//atomic.StoreInt32(&targetSegments, newTargetSegments)
			ts = atomic.LoadInt32(&targetSegments)
			//log.Println("*********************************")
			//log.Println("targetSegments: "+fmt.Sprint(ts))
			//log.Println("*********************************")
		}
		time.Sleep(time.Second * 1)

	}
}

func sendSegment(address string) int{
	log.Printf("iam here in start sendSegment now")
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

	log.Printf("iam here in start startSegmentServer now")

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
	http.HandleFunc("/updatetargetsegment", updateTargetSegmentsHandler)
	//http.handleFunc("/updateactivehostlist", updateTargetSegmentsHandler2)
	http.HandleFunc("/updateactivehostlist", updateActiveHostListLocalHandler)
	http.HandleFunc("/updateactivehostlistnewnode", updateActiveHostListNewNodeHandler)

	log.Printf("Starting segment server on %s%s\n", hostname, segmentPort)
	//log.Printf("Reachable hosts: %s", strings.Join(fetchReachableHosts()," "))
	//var tmp = fetchReachableHosts()
	//log.Printf(tmp[0])
	reachablehosts = append(reachablehosts, fetchReachableHosts()...)
	//STARTS TO PING SEGMENTS


	var ts int32
	ts = 1
	atomic.StoreInt32(&targetSegments, ts)

	log.Printf("ALSO HERE")
	go checkAll()
	log.Printf("YOUOHJDS")
	err := http.ListenAndServe(segmentPort, nil)
	if err != nil {
		log.Panic(err)
	}
	log.Printf("CHECKING: ")
	//checkAll()
}




func IndexHandler(w http.ResponseWriter, r *http.Request) {

	// We don't use the request body. But we should consume it anyway.
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	killRateGuess := 2.0

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
	var ts int32
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &ts)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
	}
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	atomic.StoreInt32(&targetSegments, ts)
}
func updateActiveHostListLocalHandler(w http.ResponseWriter, r *http.Request) {
	var host string
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &host)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%ds items): %s", pc, rateErr)
	}
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	activehosts = append(activehosts, host)
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
	//var test = "New targetSegments: "+ts
	//logger("New targetSegments: "+strconv.Itoa(int(ts)))
	//addMoreSegments(ts)
	log.Println("New targetSegments: %d", ts)
	//atomic.StoreInt32(&targetSegments, ts)
	log.Println("targetSegment: %d", atomic.LoadInt32(&targetSegments))
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


	return nodes
}

func updateActiveHostListNewNodeHandler(w http.ResponseWriter, r *http.Request){
	log.Println("AAAAAAAAAAAAAAAAA")
	var hostListString string
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &hostListString)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing updateActiveHostListNewNodeHandler (%s items): %s", pc, rateErr)
	}
	io.Copy(ioutil.Discard, r.Body)
	r.Body.Close()

	strSlice := strings.Fields(hostListString)
	log.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	log.Println("hostListString: "+hostListString)
	//log.Println("strSlice: "+strSlice)
	log.Println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
	for i:=range strSlice{
		if contains(activehosts, strSlice[i]) == false{
			activehosts = append(activehosts, strSlice[i])
		}
	}

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
	url := fmt.Sprintf("http://%s%s/", reachablehost, segmentPort)


	status, _, _ = httpGetOk(segmentClient, url)

	host := new(hoststatus)
	host.addr = reachablehost

	if status == true {
		if contains(activehosts, reachablehost) == false {
			host.status = 1
		}
	}else{
		if contains(activehosts, reachablehost) == true {
			host.status = 0
		}
	}
	channel <- *host
	log.Println("host addr["+strconv.Itoa(i)+"]: "+host.addr+" host status: "+strconv.Itoa(host.status))
	wg.Done()
}

func checkAll(){
	var wg sync.WaitGroup
	for{
		c := make(chan hoststatus, len(reachablehosts)-1)
		for i:= range reachablehosts {
			if reachablehosts[i] != hostname2{
				wg.Add(1)
				go ping(reachablehosts[i], c, &wg, i)
			}
		}


		log.Printf("+++++++++++++++++++++++++++++++++++")
		wg.Wait()
		close(c)
		updateActiveHostList1(c)
		log.Printf("-----------------------------------")

		//wg1.Wait()
	}
}
func updateActiveHostList1(channel chan hoststatus){
	//defer wg1.Done()
	var i = 0
	log.Printf("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
	for  host := range channel {

		if host.status == 1 {
			if contains(activehosts, host.addr) == false {
				activehosts = append(activehosts, host.addr)
			}
		}else{
			if contains(activehosts, host.addr) == true{
				//log.Printf("noting to remove["+strconv.Itoa(i)+"]")
				activehosts = remove(activehosts, host.addr)
			}
		}
		i = i+1
	}

}
