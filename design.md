# Design and Architecture

## Overview

**ecs-kudos-api** is a Go-based REST API that tracks GitHub contributions (pull requests and issues) as "kudos" for users. It receives GitHub webhook events, parses them, and stores contribution records in AWS DynamoDB. It is designed to be deployed as a containerized service on AWS ECS.

## Key Facts

- **Language:** Go 1.13
- **Module path:** `github.com/kohidave/ecs-kudos-api`
- **Package structure:** Single `main` package under `pkg/`
- **Deployment:** Multi-stage Docker build, runs on distroless Debian 10 base image, listens on port 80
- **Cloud Services:** AWS DynamoDB for persistence
- **Routing:** Uses `julienschmidt/httprouter`
- **GitHub Integration:** Uses `google/go-github/v29` for webhook parsing and payload validation
- **AWS SDK:** `aws/aws-sdk-go v1.29.15`

## Architecture

### API Endpoints

| Method | Path | Handler | Description |
|--------|------|---------|-------------|
| POST | `/api/contribution/gh` | `CreateContribution` | Receives GitHub webhook events (PullRequest, Issues) and stores them as kudos |
| GET | `/api/kudos/:user` | `GetKudosForUser` | Retrieves all kudos for a given GitHub user |
| GET | `/` | `HealthCheck` | Simple health check endpoint (returns 200 OK) |

### Data Model

The core data entity is `Kudo`:

- `User` (string) - GitHub username (DynamoDB partition key)
- `Time` (time.Time, stored as Unix timestamp) - When the contribution was created (DynamoDB sort key)
- `ContributionType` (string) - Either "PullRequest" or "Issue"
- `ContributionURL` (string) - Link to the GitHub contribution
- `ContributionName` (string) - Title of the PR or issue

### DynamoDB Table

- Table name pattern: `ecs-kudos-{env}-{suffix}` (currently hardcoded to `ecs-kudos-test-kudos`)
- Partition key: `User`
- Sort key: `Time` (Unix timestamp)

### Webhook Flow

1. GitHub sends a webhook POST to `/api/contribution/gh`
2. Payload is validated using a `WEBHOOK_SECRET` environment variable
3. Event type is parsed (PullRequestEvent or IssuesEvent)
4. Only "opened" actions are recorded (other actions return 202 Accepted)
5. A `Kudo` record is created and persisted to DynamoDB

### CORS

CORS is configured globally via `router.GlobalOPTIONS`:

- `Access-Control-Allow-Origin: *`
- `Access-Control-Allow-Methods: POST, GET, PUT, DELETE, OPTIONS`

### Environment Variables

| Variable | Purpose |
|----------|---------|
| `WEBHOOK_SECRET` | Secret used to validate incoming GitHub webhook payloads |
| AWS credentials | Standard AWS SDK credential chain (shared config enabled) |

### Docker Build

- Multi-stage build: Go 1.13 (buster) for compilation, distroless for runtime
- Binary name: `kudos-api`
- Uses `GOPROXY=direct` to bypass Go module proxy
- Exposes port 80

### Source Files

| File | Purpose |
|------|---------|
| `pkg/main.go` | HTTP server setup, route handlers, webhook processing logic |
| `pkg/kudos.go` | DynamoDB service layer (KudosService), data model (Kudo struct) |
| `Dockerfile` | Multi-stage container build |
| `go.mod` / `go.sum` | Go module dependencies |

## Design Observations

1. **No tests** - The repository has no test files.
2. **Hardcoded environment** - The DynamoDB table name uses a hardcoded "test" environment rather than reading from an environment variable.
3. **No middleware** - No request logging, authentication (beyond webhook validation), or rate limiting middleware.
4. **Session per request** - AWS sessions are created per-request in handlers rather than being shared at application startup.
5. **Single commit** - Repository has minimal git history (1 commit visible).
6. **No CI/CD configuration** - No GitHub Actions, Makefile, or deployment scripts present.
7. **Outdated dependencies** - Go 1.13 and early 2020 dependency versions.
