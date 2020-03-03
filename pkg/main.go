package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/google/go-github/v29/github"
	"github.com/julienschmidt/httprouter"
)

// CreateContribution records a GH contribution
func CreateContribution(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {

	awsSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	kudosService := NewKudosService(awsSess)

	payload, err := github.ValidatePayload(req, []byte(os.Getenv("WEBHOOK-SECRET")))
	if err != nil {
		log.Printf("error validating request body: err=%s\n", err)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	defer req.Body.Close()

	event, err := github.ParseWebHook(github.WebHookType(req), payload)
	if err != nil {
		log.Printf("could not parse webhook: err=%s\n", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	switch e := event.(type) {
	case *github.PullRequestEvent:
		if e.GetAction() != "opened" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		req := &Kudo{
			User:             e.GetPullRequest().GetUser().GetLogin(),
			Time:             e.GetPullRequest().GetCreatedAt(),
			ContributionType: "PullRequest",
			ContributionURL:  e.GetPullRequest().GetURL(),
			ContributionName: e.GetPullRequest().GetTitle(),
		}
		if err := kudosService.CreateKudo(req); err != nil {
			log.Printf("🚨 could create kudo: err=%s\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	case *github.IssuesEvent:
		if e.GetAction() != "opened" {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		req := &Kudo{
			User:             e.GetIssue().GetUser().GetLogin(),
			Time:             e.GetIssue().GetCreatedAt(),
			ContributionType: "Issue",
			ContributionURL:  e.GetIssue().GetURL(),
			ContributionName: e.GetIssue().GetTitle(),
		}
		if err := kudosService.CreateKudo(req); err != nil {
			log.Printf("🚨 could create kudo: err=%s\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		return
	default:
		log.Printf("🤷‍♀️ event type %s\n", github.WebHookType(req))
		return
	}
}

// GetKudosForUser fetches the kudos for a particular user
func GetKudosForUser(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	user := ps.ByName("user")

	awsSess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	kudosService := NewKudosService(awsSess)
	kudos, err := kudosService.GetKudos(user)
	if err != nil {
		log.Printf("🚨 could fetch kudos: err=%s\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	serializedResponse, _ := json.Marshal(kudos)
	w.Header().Set("Content-Type", "application/json")
	w.Write(serializedResponse)
	return
}

// HealthCheck just returns true if the service is up.
func HealthCheck(w http.ResponseWriter, req *http.Request, ps httprouter.Params) {
	w.WriteHeader(http.StatusOK)
}

func main() {

	router := httprouter.New()

	// Webhooks endpoint
	router.POST("/api/contribution/gh", CreateContribution)
	router.GET("/api/kudos/:user", GetKudosForUser)

	// Health Check
	router.GET("/", HealthCheck)

	router.GlobalOPTIONS = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Set CORS headers
		header := w.Header()
		header.Set("Access-Control-Allow-Origin", "*")
		header.Set("Access-Control-Allow-Headers", "X-Requested-With")
		header.Set("Access-Control-Allow-Methods", "POST, GET, PUT, DELETE, OPTIONS")

		// Adjust status code to 204
		w.WriteHeader(http.StatusNoContent)
	})

	log.Fatal(http.ListenAndServe(":80", router))
}
