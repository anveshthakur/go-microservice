package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/rpc"
	"time"

	"github.com/anveshthakur/broker/event"
	"github.com/anveshthakur/broker/logs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RequestPayload struct {
	Action string      `json:"action"`
	Auth   AuthPayload `json:"auth,omitempty"`
	Log    LogPayload  `json:"log,omitempty"`
	Mail   MailPayload `json:"mail,omitempty"`
}

type MailPayload struct {
	From    string `json:"from"`
	To      string `json:"to"`
	Subject string `json:"subject"`
	Message string `json:"message"`
}

type AuthPayload struct {
	Email    string `json:"email"`
	Password string `json:pasword"`
}

type LogPayload struct {
	Name string `json:"name"`
	Data string `json:"data"`
}

func (app *config) Broker(w http.ResponseWriter, r *http.Request) {
	payload := JsonResponse{
		Error:   false,
		Message: "Hit the broker",
	}

	_ = app.writeJSON(w, http.StatusOK, payload)
}

func (app *config) HandleSubmission(w http.ResponseWriter, r *http.Request) {
	var rp RequestPayload
	err := app.readJSON(w, r, &rp)
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	switch rp.Action {
	case "auth":
		app.authenicate(w, rp.Auth)
	case "log":
		app.logItemViaRPC(w, rp.Log)
	case "mail":
		app.sendMail(w, rp.Mail)
	default:
		app.errorJSON(w, errors.New("unknown action"))
	}
}

func (app *config) logEventRabbit(w http.ResponseWriter, l LogPayload) {
	err := app.pushToQueue(l.Name, l.Data)
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	var payload JsonResponse
	payload.Error = false
	payload.Message = "logged via RabbitMQ"

	app.writeJSON(w, http.StatusAccepted, payload)
}

func (app *config) pushToQueue(name, msg string) error {
	emmiter, err := event.NewEventEmitter(app.Rabbit)
	if err != nil {
		return err
	}

	payload := LogPayload{
		Name: name,
		Data: msg,
	}

	j, _ := json.MarshalIndent(&payload, "", "\t")
	err = emmiter.Push(string(j), "LOG.INFO")
	if err != nil {
		return err
	}
	return nil
}

func (app *config) logItem(w http.ResponseWriter, entry LogPayload) {
	jd, _ := json.MarshalIndent(entry, "", "\t")
	logServiceurl := "http://logger-service/log"
	req, err := http.NewRequest("POST", logServiceurl, bytes.NewBuffer(jd))
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		app.errorJSON(w, err)
		return
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		app.errorJSON(w, err)
		return
	}
	var payload JsonResponse
	payload.Error = false
	payload.Message = "Logged"

	app.writeJSON(w, http.StatusAccepted, payload)
}

func (app *config) authenicate(w http.ResponseWriter, a AuthPayload) {
	// create JSON and send it to auth microservices
	jd, _ := json.MarshalIndent(a, "", "\t")

	// call the service
	r, err := http.NewRequest("POST", "http://authentication-service/authenticate", bytes.NewBuffer(jd))
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	client := &http.Client{}
	res, err := client.Do(r)
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	log.Println(res.Body)

	defer res.Body.Close()

	// make sure we get the correct status codes
	if res.StatusCode == http.StatusUnauthorized {
		app.errorJSON(w, errors.New("invalid authorization"))
		return
	} else if res.StatusCode != http.StatusAccepted {
		app.errorJSON(w, errors.New("error calling service"))
		return
	}
	// var for reading response.body
	var jsonFromService JsonResponse

	// decode the json
	err = json.NewDecoder(res.Body).Decode(&jsonFromService)
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	if jsonFromService.Error {
		app.errorJSON(w, err, http.StatusUnauthorized)
		return
	}

	payload := JsonResponse{
		Error:   false,
		Message: "Authenticated",
		Data:    jsonFromService.Data,
	}

	app.writeJSON(w, http.StatusAccepted, payload)
}

func (app *config) sendMail(w http.ResponseWriter, m MailPayload) {
	jsonData, _ := json.MarshalIndent(m, "", "\t")
	mailServiceURL := "http://mailer-service/send"
	request, err := http.NewRequest("POST", mailServiceURL, bytes.NewBuffer(jsonData))
	if err != nil {
		app.errorJSON(w, err)
	}

	request.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	defer response.Body.Close()
	if response.StatusCode != http.StatusAccepted {
		app.errorJSON(w, errors.New("error calling mail service"))
		return
	}

	var payload JsonResponse
	payload.Error = false
	payload.Message = "Message sent to " + m.To

	app.writeJSON(w, http.StatusAccepted, payload)
}

type RPCPayload struct {
	Name string
	Data string
}

func (app *config) logItemViaRPC(w http.ResponseWriter, l LogPayload) {
	client, err := rpc.Dial("tcp", "logger-service:5001")
	if err != nil {
		app.errorJSON(w, err)
	}

	rpcPayload := RPCPayload{
		Name: l.Name,
		Data: l.Data,
	}

	var result string
	err = client.Call("RPCServer.LogInfo", rpcPayload, &result)
	if err != nil {
		app.errorJSON(w, err)
		return
	}

	payload := JsonResponse{
		Error:   false,
		Message: result,
	}

	app.writeJSON(w, http.StatusAccepted, payload)
}

func (app *config) LogViaGRPC(w http.ResponseWriter, r *http.Request) {
	var requestPayload RequestPayload

	if err := app.readJSON(w, r, &requestPayload); err != nil {
		app.errorJSON(w, err)
		return
	}

	conn, err := grpc.Dial("logger-service:50001", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		app.errorJSON(w, err)
		return
	}
	defer conn.Close()

	c := logs.NewLogServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = c.WriteLog(ctx, &logs.LogRequest{
		LogEntry: &logs.Log{
			Name: requestPayload.Log.Name,
			Data: requestPayload.Log.Data,
		},
	})

	if err != nil {
		app.errorJSON(w, err)
		return
	}

	app.writeJSON(w, http.StatusAccepted, JsonResponse{
		Error:   false,
		Message: "Logged!!",
	})
}
