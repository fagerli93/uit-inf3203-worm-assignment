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
)

var wormgatePort string
var segmentPort string

var hostname string

var targetSegments int32


var reachablehosts []string

var activehosts []string




func main() {

	hostname, _ = os.Hostname()
	log.SetPrefix(hostname + " segment: ")

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
		logger("test")
		sendSegment(*spreadHost)
	case "run":
		runMode.Parse(os.Args[2:])
		activehosts = append(activehosts, hostname)
		startSegmentServer()

	default:
		log.Fatalf("Unknown mode %q\n", os.Args[1])
	}
}

func addCommonFlags(flagset *flag.FlagSet) {
	flagset.StringVar(&wormgatePort, "wp", ":8181", "wormgate port (prefix with colon)")
	flagset.StringVar(&segmentPort, "sp", ":8182", "segment port (prefix with colon)")
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
func updateActiveSegmentsWithNewInfo(newts int32, newnode string){

	for i:= range activehosts {
		if(activehosts[i] != hostname && activehosts[i] != newnode){
			updateTargetSegments(newts, activehosts[i])
			updateActiveHostListRemote(newnode, activehosts[i])
		}
	}
}


var segmentClient *http.Client

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

func findFreeNodeAddress()string{
	for i := range reachablehosts{
		for j := range activehosts{
			if(reachablehosts[i] != hostname && reachablehosts[i] != activehosts[j]){
				return reachablehosts[i]
			}
		}
	}
	return "ERROR"
}
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
	updateTargetSegments(newTargetSegments, new_node)
	//update all other active nodes with ts and new active segment
	for i :=range activehosts{
		if(activehosts[i] != hostname){
			updateTargetSegments(newTargetSegments, activehosts[i])
			updateActiveHostListRemote(new_node, activehosts[i])
		}else{
			atomic.StoreInt32(&targetSegments, newTargetSegments)
			activehosts = append(activehosts, new_node)
		}
	}
}


func addMoreSegments(newTargetSegments int32){
	for targetSegments < newTargetSegments{
		new_node := findFreeNodeAddress()
		resp := sendSegment(new_node)
		if(resp == 200){
			update(targetSegments+1, new_node)
		}
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
	http.HandleFunc("/", IndexHandler)
	http.HandleFunc("/targetsegments", targetSegmentsHandler)
	http.HandleFunc("/shutdown", shutdownHandler)
	http.HandleFunc("/updatetargetsegment", updateTargetSegmentsHandler)
	//http.handleFunc("/updateactivehostlist", updateTargetSegmentsHandler2)
	http.HandleFunc("/updateactivehostlist", updateActiveHostListLocal)

	log.Printf("Starting segment server on %s%s\n", hostname, segmentPort)
	log.Printf("Reachable hosts: %s", strings.Join(fetchReachableHosts()," "))
	//var tmp = fetchReachableHosts()
	//log.Printf(tmp[0])
	reachablehosts = append(reachablehosts, fetchReachableHosts()...)



	err := http.ListenAndServe(segmentPort, nil)
	if err != nil {
		log.Panic(err)
	}
}




func IndexHandler(w http.ResponseWriter, r *http.Request) {

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
func updateActiveHostListLocal(w http.ResponseWriter, r *http.Request) {
	var host string
	pc, rateErr := fmt.Fscanf(r.Body, "%d", &host)
	if pc != 1 || rateErr != nil {
		log.Printf("Error parsing targetSegments (%d items): %s", pc, rateErr)
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
	addMoreSegments(ts)
	log.Println("New targetSegments: %d", ts)
	atomic.StoreInt32(&targetSegments, ts)
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
