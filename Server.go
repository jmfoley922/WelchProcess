package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"runtime"
	"strconv"
)

type Settings struct {
	LocalIp      string `json:"localIp"`
	Port         int    `json:"port"`
	SqlServer    string `json:"sqlServer"`
	Db           string `json:"db"`
	Instance     string `json:"instance"`
	DbServerPort int    `json:"dbServerPort"`
	DbUser       string `json:"dbUser"`
	DbPassword   string `json:"dbPassword"`
}

var appSettings = Settings{}

//Get the setings from the file
func getSettings() (err error) {

	settingsStr, err := ioutil.ReadFile("./welch_settings.json")

	err = json.Unmarshal(settingsStr, &appSettings)

	if err != nil {
		fmt.Printf("Error = %s\n", err)
		return err
	}

	return err

}

//Build the Sql server connection string from the settings file
func GetConnectionString() string {

	var connString = fmt.Sprintf("server=%s;user id=%s;password=%s;encrypt=%s;database=%s;port=%d", appSettings.SqlServer, appSettings.DbUser, appSettings.DbPassword,
		"disable", appSettings.Db, appSettings.DbServerPort)

	return connString
}

//Process a kiosk trans from the client
func kioskTrans(w http.ResponseWriter, r *http.Request) {

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Printf("Error = %s", err)
		io.WriteString(w, "OK")
	}

	defer r.Body.Close()

	outData := ProcessKioskTrans(data)

	io.WriteString(w, outData)

}

//Respond to a comm check message
func commCheck(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "OK")
}

func main() {

	runtime.GOMAXPROCS(2)

	err := getSettings()
	if err != nil {
		log.Fatal("Error reading settings file: " + err.Error())
	}

	http.HandleFunc("/kioskdata", kioskTrans)
	http.HandleFunc("/commcheck", commCheck)

	log.Println("Welch process server starting at " + appSettings.LocalIp + " on port " + strconv.Itoa(appSettings.Port))
	log.Fatal(http.ListenAndServeTLS(appSettings.LocalIp+":"+strconv.Itoa(appSettings.Port), "./m3-cert.pem", "./m3key.pem", nil)) //https
}
