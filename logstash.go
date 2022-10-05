package logstash

import (
	// "crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unsafe"

	docker "github.com/fsouza/go-dockerclient"
	"github.com/gliderlabs/logspout/router"
)

func init() {
	router.AdapterFactories.Register(NewLogstashAdapter, "logstash")
}

// LogstashAdapter is an adapter that streams UDP JSON to Logstash.
type LogstashAdapter struct {
	conn           net.Conn
	route          *router.Route
	containerTags  map[string][]string
	logstashFields map[string]map[string]string
	decodeJsonLogs map[string]bool
}

// NewLogstashAdapter creates a LogstashAdapter with UDP as the default transport.
func NewLogstashAdapter(route *router.Route) (router.LogAdapter, error) {
	transport, found := router.AdapterTransports.Lookup(route.AdapterTransport("udp"))
	if !found {
		return nil, errors.New("unable to find adapter: " + route.Adapter)
	}

	for {
		conn, err := transport.Dial(route.Address, route.Options)

		if err == nil {

			log.Println("logstash: conn:")
			fmt.Printf("%+v\n", conn)
			connOut, _ := json.Marshal(conn)
			fmt.Println(string(connOut))
			log.Println("logstash: -----")

			return &LogstashAdapter{
				route:          route,
				conn:           conn,
				containerTags:  make(map[string][]string),
				logstashFields: make(map[string]map[string]string),
				decodeJsonLogs: make(map[string]bool),
			}, nil
		}
		if os.Getenv("RETRY_STARTUP") == "" {
			return nil, err
		}
		log.Println("Retrying:", err)
		time.Sleep(2 * time.Second)
	}
}

// Get container tags configured with the environment variable LOGSTASH_TAGS
func GetContainerTags(c *docker.Container, a *LogstashAdapter) []string {
	if tags, ok := a.containerTags[c.ID]; ok {
		return tags
	}

	tags := []string{}
	tagsStr := os.Getenv("LOGSTASH_TAGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_TAGS=") {
			tagsStr = strings.TrimPrefix(e, "LOGSTASH_TAGS=")
			break
		}
	}

	if len(tagsStr) > 0 {
		tags = strings.Split(tagsStr, ",")
	}

	a.containerTags[c.ID] = tags
	return tags
}

// Get logstash fields configured with the environment variable LOGSTASH_FIELDS
func GetLogstashFields(c *docker.Container, a *LogstashAdapter) map[string]string {
	if fields, ok := a.logstashFields[c.ID]; ok {
		return fields
	}

	fieldsStr := os.Getenv("LOGSTASH_FIELDS")
	fields := map[string]string{}

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "LOGSTASH_FIELDS=") {
			fieldsStr = strings.TrimPrefix(e, "LOGSTASH_FIELDS=")
		}
	}

	if len(fieldsStr) > 0 {
		for _, f := range strings.Split(fieldsStr, ",") {
			sp := strings.Split(f, "=")
			k, v := sp[0], sp[1]
			fields[k] = v
		}
	}

	a.logstashFields[c.ID] = fields

	return fields
}

// Get boolean indicating whether json logs should be decoded (or added as message),
// configured with the environment variable DECODE_JSON_LOGS
func IsDecodeJsonLogs(c *docker.Container, a *LogstashAdapter) bool {
	if decodeJsonLogs, ok := a.decodeJsonLogs[c.ID]; ok {
		return decodeJsonLogs
	}

	decodeJsonLogsStr := os.Getenv("DECODE_JSON_LOGS")

	for _, e := range c.Config.Env {
		if strings.HasPrefix(e, "DECODE_JSON_LOGS=") {
			decodeJsonLogsStr = strings.TrimPrefix(e, "DECODE_JSON_LOGS=")
		}
	}

	decodeJsonLogs := decodeJsonLogsStr != "false"

	a.decodeJsonLogs[c.ID] = decodeJsonLogs

	return decodeJsonLogs
}

// Get hostname of container, searching first for /etc/host_hostname, otherwise
// using the hostname assigned to the container (typically container ID).
func GetContainerHostname(c *docker.Container) string {
	content, err := ioutil.ReadFile("/etc/host_hostname")
	if err == nil && len(content) > 0 {
		return strings.Trim(string(content), "\r\n")
	}

	return c.Config.Hostname
}

// Stream implements the router.LogAdapter interface.
func (a *LogstashAdapter) Stream(logstream chan *router.Message) {

	for m := range logstream {

		dockerInfo := DockerInfo{
			Name:     m.Container.Name,
			ID:       m.Container.ID,
			Image:    m.Container.Config.Image,
			Hostname: GetContainerHostname(m.Container),
		}

		// Check if we are sending logs for this container
		if !containerIncluded(dockerInfo.Name) {
			continue
		}

		if os.Getenv("DOCKER_LABELS") != "" {
			dockerInfo.Labels = make(map[string]string)
			for label, value := range m.Container.Config.Labels {
				dockerInfo.Labels[strings.Replace(label, ".", "_", -1)] = value
			}
		}

		tags := GetContainerTags(m.Container, a)
		fields := GetLogstashFields(m.Container, a)

		var js []byte
		var data map[string]interface{}
		var err error

		// Try to parse JSON-encoded m.Data. If it wasn't JSON, create an empty object
		// and use the original data as the message.
		if IsDecodeJsonLogs(m.Container, a) {
			err = json.Unmarshal([]byte(m.Data), &data)
		}
		if err != nil || data == nil {
			data = make(map[string]interface{})
			data["message"] = m.Data
		}

		for k, v := range fields {
			data[k] = v
		}

		data["docker"] = dockerInfo
		data["stream"] = m.Source
		data["tags"] = tags

		// Return the JSON encoding
		if js, err = json.Marshal(data); err != nil {
			// Log error message and continue parsing next line, if marshalling fails
			log.Println("logstash: could not marshal JSON:", err)
			continue
		}

		// To work with tls and tcp transports via json_lines codec
		js = append(js, byte('\n'))

		log.Println("logstash: logging [start] -> ", m.Data)

		for {

			_, err := a.conn.Write(js)

			log.Println("logstash: a:")
			fmt.Printf("%+v\n", a)
			if aOut, err := json.Marshal(a); err != nil {
				log.Println("logstash: Failed to marshal a")
			} else {
				fmt.Println(string(aOut))
			}

			log.Println("logstash: a.conn:")
			fmt.Printf("%+v\n", a.conn)
			if connOut, err := json.Marshal(a.conn); err != nil {
				log.Println("logstash: Failed to marshal a.conn")
			} else {
				fmt.Println(string(connOut))
			}

			log.Println("logstash: a reflection: ", reflect.ValueOf(a).Kind())
			log.Println("logstash: *a reflection: ", reflect.ValueOf(*a).Kind())
			log.Println("logstash: &*a Elem reflection: ", reflect.ValueOf(&*a).Elem())
			ac := a.conn
			log.Println("logstash: Kind a.conn reflection: ", reflect.ValueOf(ac).Kind())
			log.Println("logstash: Type a.conn reflection: ", reflect.ValueOf(ac).Type())
			log.Println("logstash: Kind & a.conn reflection: ", reflect.ValueOf(&ac).Kind())
			log.Println("logstash: Elem a.conn reflection: ", reflect.ValueOf(ac).Elem())
			//log.Println("logstash: Elem.conn.Elem a.conn reflection: ", reflect.ValueOf(ac).Elem().conn.Elem())
			log.Println("logstash: Elem.Type a.conn reflection: ", reflect.ValueOf(ac).Elem().Type())
			log.Println("logstash: Elem.Kind a.conn reflection: ", reflect.ValueOf(ac).Elem().Kind())
			log.Println("logstash: Elem.NumField a.conn reflection: ", reflect.ValueOf(ac).Elem().NumField())
			
			// log.Println("create rs")
			// rs := reflect.ValueOf(ac).Elem()
			// log.Println("create rs2")
			// rs2 := reflect.New(rs.Type()).Elem()
			// log.Println("set rs2")
			// rs2.Set(rs)
			// log.Println("create rf")
			// rf := rs2.Field(0)
			// log.Println("set rf")
			// rf = reflect.NewAt(rf.Type(), unsafe.Pointer(rf.UnsafeAddr())).Elem()			
			// log.Println("magic done")

			//ac := a.conn
			log.Println("create s")
			s := reflect.ValueOf(ac).Elem()
			log.Println("create typeOfT")
			typeOfT := s.Type()
			
			log.Println("get NumField")
			for i := 0; i < s.NumField(); i++ {
				log.Println("get Field")
				f := s.Field(i)
				log.Println("fmt.Printf")
				fmt.Printf("%d: %s %s\n", i,
					typeOfT.Field(i).Name, f.Type())
				log.Println("reflect.NewAt")
				f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()			
				log.Println("fmt.Printf")
				fmt.Printf("%d: %s %s = %v\n", i,
					typeOfT.Field(i).Name, f.Type(), f.Interface())
				log.Println("loop end")
			}			

			log.Println("get NumMethod: ", typeOfT.NumMethod())
			for i := 0; i < s.NumMethod(); i++ {
				log.Println("get Method")
				m := typeOfT.Method(i)
				//log.Println("reflect.NewAt")
				//f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()			
				log.Println("fmt.Printf")
				fmt.Printf("%d: %s %s\n", i,
					m.Name, m.Type)
				log.Println("loop end")
			}			

			// log.Println("logstash: a.conn reflection:")
			// ac := a.conn
			// s := reflect.ValueOf(&ac).Elem()
			// typeOfT := s.Type()
			// 
			// for i := 0; i < s.NumField(); i++ {
			// 	f := s.Field(i)
			// 	fmt.Printf("%d: %s %s = %v\n", i,
			// 		typeOfT.Field(i).Name, f.Type(), f.Interface())
			// }			

			log.Println("logstash: -----")
			//log.Println("logstash: logging [isConnected] -> ", a.conn.isConnected)
			log.Println("logstash: logging [end] -> ", m.Data)
			log.Println("logstash: logging Write -> ", err)
			if err == nil {
				break
			}

			if os.Getenv("RETRY_SEND") == "" {
				log.Fatal("logstash: could not write:", err)
			} else {
				time.Sleep(2 * time.Second)
			}
		}
	}
}

// containerIncluded Returns true if this container is in INCLUDE_CONTAINERS, or not env var is set
func containerIncluded(inputContainerName string) bool {
	if includeContainers := os.Getenv("INCLUDE_CONTAINERS"); includeContainers != "" {
		for _, containerName := range strings.Split(includeContainers, ",") {
			if inputContainerName == containerName {
				// This contain is included, send this log
				return true
			}
		}
		return false
	} else if includeContainers := os.Getenv("INCLUDE_CONTAINERS_REGEX"); includeContainers != "" {
		compiled, err := regexp.Compile(includeContainers)
		if err != nil {
			// Return true by default
			return true
		}
		if compiled.MatchString(inputContainerName) {
			// This contain is included, send this log
			return true
		}
		return false
	}
	return true
}

type DockerInfo struct {
	Name     string            `json:"name"`
	ID       string            `json:"id"`
	Image    string            `json:"image"`
	Hostname string            `json:"hostname"`
	Labels   map[string]string `json:"labels"`
}
