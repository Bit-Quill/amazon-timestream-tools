package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/grafana"
	grafanaTypes "github.com/aws/aws-sdk-go-v2/service/grafana/types"
)

func getWorkspaceByName(grafanaClient *grafana.Client, workspaceName string) (*grafanaTypes.WorkspaceSummary, error) {
	const SleepDuration = 5
	const MaxWaitIntervals = 200
	waiterInterval := 0

	resp, err := grafanaClient.ListWorkspaces(context.TODO(), &grafana.ListWorkspacesInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list workspaces: %v", err)
	}

	if resp == nil {
		for waiterInterval < MaxWaitIntervals {
			resp, err = grafanaClient.ListWorkspaces(context.TODO(), &grafana.ListWorkspacesInput{})
			if resp != nil {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("failed to list workspaces: %v", err)
			}
			waiterInterval++
			time.Sleep(SleepDuration * time.Second)
		}
	}

	if waiterInterval == MaxWaitIntervals {
		return nil, fmt.Errorf("failed to find workspace %s in workspaces", workspaceName)
	}

	for _, workspace := range resp.Workspaces {
		if *workspace.Name == workspaceName {
			return &workspace, nil
		}
	}

	return nil, fmt.Errorf("failed to find workspace %s in workspaces", workspaceName)
}

func getGrafanaHttpReq(requestType string, urlWithEndpoint string, payload io.Reader, serviceAccountTokenKey string) (*http.Request, error) {
	req, err := http.NewRequest(requestType, "https://"+urlWithEndpoint, payload)
	if err != nil {
		log.Printf("Failed to create Grafana HTTP request: %s", err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+serviceAccountTokenKey)
	return req, nil
}

func sendGrafanaHttpReq(httpClient http.Client, req *http.Request) (*http.Response, error) {
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Printf("Failed to execute Grafana HTTP request: %s", err)
		return nil, err
	}
	return resp, nil
}

func uploadDashboard(serviceAccountTokenKey string, workspaceUrl string, datasourceName string, dashboardName string, dbInstanceNames string) (string, error) {
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	dataSourceConfig := map[string]interface{}{
		"name":   datasourceName,
		"type":   "cloudwatch",
		"access": "proxy",
		"jsonData": map[string]interface{}{
			"authType":      "default",
			"defaultRegion": os.Getenv("AWS_REGION"),
		},
	}

	jsonDataSourceConfig, err := json.Marshal(dataSourceConfig)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %v", err)
	}

	configureGrafanaDatasourceReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/datasources",
		bytes.NewBuffer(jsonDataSourceConfig),
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}
	configureGrafanaDatasourceResp, err := sendGrafanaHttpReq(*httpClient, configureGrafanaDatasourceReq)
	if err != nil {
		log.Printf("Failed to configure CloudWatch data source: %s", err)
		return "", err
	}

	if configureGrafanaDatasourceResp.StatusCode == http.StatusOK {
		log.Printf("CloudWatch data source successfully added to workspace")
	} else if configureGrafanaDatasourceResp.StatusCode == http.StatusConflict {
		log.Printf("CloudWatch data source already exists in workspace")
	} else {
		log.Printf("Failed to add CloudWatch data source to workspace with status code %d", configureGrafanaDatasourceResp.StatusCode)
		return "", fmt.Errorf("failed to add CloudWatch data source: %d", configureGrafanaDatasourceResp.StatusCode)
	}

	jsonDashboard, err := json.Marshal(generateDashboard(datasourceName, dashboardName, dbInstanceNames))
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %v", err)
	}

	uploadGrafanaDashboardReq, err := getGrafanaHttpReq(
		"POST",
		workspaceUrl+"/api/dashboards/db",
		bytes.NewBuffer(jsonDashboard),
		serviceAccountTokenKey,
	)
	if err != nil {
		return "", err
	}
	uploadGrafanaDashboardResp, err := sendGrafanaHttpReq(*httpClient, uploadGrafanaDashboardReq)
	if err != nil {
		log.Printf("Failed to upload Grafana dashboard: %s", err)
		return "", err
	}

	if uploadGrafanaDashboardResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("error status code returned from uploading dashboard: %d", uploadGrafanaDashboardResp.StatusCode)
	}

	return "Dashboard created successfully", nil
}

func createGrafanaDashboard() (string, error) {
	const SleepDuration = 5
	const MaxWaitIntervals = 200

	workspaceName := os.Getenv("GrafanaWorkspaceName")
	if workspaceName == "" {
		return "", fmt.Errorf("Failed to get GrafanaWorkspaceName environment variable")
	}
	datasourceName := os.Getenv("CloudWatchDatasourceName")
	if datasourceName == "" {
		return "", fmt.Errorf("Failed to get CloudWatchDatasourceName environment variable")
	}
	dashboardName := os.Getenv("DashboardName")
	if dashboardName == "" {
		return "", fmt.Errorf("Failed to get DashboardName environment variable")
	}
	dbInstanceNames := os.Getenv("DbInstanceNames")
	if dbInstanceNames == "" {
		return "", fmt.Errorf("Failed to get DbInstanceNames environment variable")
	}

	awsConfig, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Printf("error loading default AWS config: %s", err)
		return "", err
	}

	grafanaServiceAccountTokenName := "ADMIN"
	var grafanaServiceAccountTokenSecondsToLive int32 = 86400 //24 hours
	grafanaClient := grafana.NewFromConfig(awsConfig)

	serviceAccountTokenKey := ""
	grafanaWorkspace, err := getWorkspaceByName(grafanaClient, workspaceName)
	if err != nil {
		log.Printf("Failed to get workspace: %s", err)
		return "", err
	}

	waiterInterval := 0

	if grafanaWorkspace.Status != grafanaTypes.WorkspaceStatusActive {
		for waiterInterval < MaxWaitIntervals {
			grafanaWorkspace, err = getWorkspaceByName(grafanaClient, workspaceName)
			if err != nil {
				log.Printf("Failed to get workspace: %s", err)
				return "", err
			}
			if grafanaWorkspace.Status == grafanaTypes.WorkspaceStatusActive {
				break
			}
			waiterInterval++
			time.Sleep(SleepDuration * time.Second)
		}
	}

	if waiterInterval == MaxWaitIntervals {
		return "", fmt.Errorf("Failed to create service account due to workspace creation timeout")
	}

	var maxWorkspaceServiceAccountsResult int32 = 200
	workspaceServiceAccounts, err := grafanaClient.ListWorkspaceServiceAccounts(
		context.TODO(),
		&grafana.ListWorkspaceServiceAccountsInput{
			WorkspaceId: grafanaWorkspace.Id,
			MaxResults:  &maxWorkspaceServiceAccountsResult,
		},
	)
	if err != nil {
		log.Printf("Error listing workspace service accounts: %s", err)
		return "", err
	}

	serviceAccountID := ""
	for _, serviceAccount := range workspaceServiceAccounts.ServiceAccounts {
		if *serviceAccount.Name == workspaceName {
			serviceAccountID = *serviceAccount.Id
			break
		}
	}

	if serviceAccountID == "" {
		serviceAccountOutput, err := grafanaClient.CreateWorkspaceServiceAccount(
			context.TODO(),
			&grafana.CreateWorkspaceServiceAccountInput{
				GrafanaRole: grafanaTypes.RoleAdmin,
				Name:        &workspaceName,
				WorkspaceId: grafanaWorkspace.Id,
			},
		)
		if err != nil {
			log.Printf("Error listing workspace service accounts: %s", err)
			return "", err
		}
		serviceAccountID = *serviceAccountOutput.Id
	} else {
		serviceAccountTokens, err := grafanaClient.ListWorkspaceServiceAccountTokens(context.TODO(), &grafana.ListWorkspaceServiceAccountTokensInput{
			ServiceAccountId: &serviceAccountID,
			WorkspaceId:      grafanaWorkspace.Id,
			MaxResults:       &maxWorkspaceServiceAccountsResult,
		})
		if err != nil {
			log.Printf("Error listing workspace service account tokens: %s", err)
			return "", err
		}
		for _, serviceAccountToken := range serviceAccountTokens.ServiceAccountTokens {
			if *serviceAccountToken.Name == "ADMIN" {
				log.Printf("Existing service account token exists which needs to be deleted and re-created")
				grafanaClient.DeleteWorkspaceServiceAccountToken(context.TODO(), &grafana.DeleteWorkspaceServiceAccountTokenInput{
					ServiceAccountId: &serviceAccountID,
					TokenId:          serviceAccountToken.Id,
					WorkspaceId:      grafanaWorkspace.Id,
				})
				break
			}
		}
	}

	serviceAccountTokenOutput, err := grafanaClient.CreateWorkspaceServiceAccountToken(context.TODO(), &grafana.CreateWorkspaceServiceAccountTokenInput{
		Name:             &grafanaServiceAccountTokenName,
		SecondsToLive:    &grafanaServiceAccountTokenSecondsToLive,
		ServiceAccountId: &serviceAccountID,
		WorkspaceId:      grafanaWorkspace.Id,
	})
	if err != nil {
		log.Printf("Error getting Grafana service account token: %s", err)
		return "", err
	}
	if serviceAccountTokenOutput.ServiceAccountToken.Key == nil {
		log.Printf("Error getting Grafana service account token: %s", err)
		return "", fmt.Errorf("Failed to get token key")
	}
	serviceAccountTokenKey = *serviceAccountTokenOutput.ServiceAccountToken.Key

	// The endpoint may take time to populate from the workspace output
	waiterInterval = 0
	for waiterInterval < MaxWaitIntervals {
		grafanaWorkspace, err = getWorkspaceByName(grafanaClient, workspaceName)
		if err != nil {
			log.Printf("Failed to get workspace: %s", err)
			return "", err
		}
		if grafanaWorkspace.Endpoint != nil {
			break
		}
		waiterInterval++
		time.Sleep(SleepDuration * time.Second)
	}

	ret, err := uploadDashboard(serviceAccountTokenKey, *grafanaWorkspace.Endpoint, datasourceName, dashboardName, dbInstanceNames)
	if err != nil {
		return "", err
	}

	return ret, nil
}

func lambdaHandler(ctx context.Context, event map[string]interface{}) (events.APIGatewayProxyResponse, error) {
	resp, err := createGrafanaDashboard()
	if err != nil {
		log.Printf("Error creating Grafana dashboard: %v", err)
		return events.APIGatewayProxyResponse{
			StatusCode: http.StatusInternalServerError,
			Body:       fmt.Sprintf("Error creating Grafana dashboard: %v", err),
		}, nil
	}

	return events.APIGatewayProxyResponse{
		StatusCode: http.StatusOK,
		Body:       resp,
	}, nil
}

func main() {
	lambda.Start(lambdaHandler)
}

type panelField struct {
	gridPosition  map[string]interface{}
	title         string
	panelType     string
	refId         string
	statisticType string
	metricName    string
}

func generatePanelOptions(panelType string) map[string]interface{} {
	switch panelType {
	case "stat":
		return map[string]interface{}{
			"colorMode":   "value",
			"graphMode":   "area",
			"justifyMode": "auto",
			"orientation": "auto",
			"reduceOptions": map[string]interface{}{
				"calcs": []string{
					"lastNotNull",
				},
				"fields": "",
				"values": false,
			},
			"showPercentChange": false,
			"textMode":          "auto",
			"wideLayout":        true,
		}
	case "bargauge":
		return map[string]interface{}{
			"minVizHeight":  16,
			"minVizWidth":   8,
			"maxVizHeight":  300,
			"orientation":   "auto",
			"displayMode":   "gradient",
			"valueMode":     "color",
			"namePlacement": "auto",
			"reduceOptions": map[string]interface{}{
				"calcs": []string{
					"lastNotNull",
				},
				"fields": "",
				"values": false,
			},
			"showUnfilled":         true,
			"showThresholdLabels":  false,
			"showThresholdMarkers": true,
			"sizing":               "auto",
		}
	case "gauge":
		return map[string]interface{}{
			"minVizHeight": 75,
			"minVizWidth":  75,
			"orientation":  "auto",
			"reduceOptions": map[string]interface{}{
				"calcs": []string{
					"lastNotNull",
				},
				"fields": "",
				"values": false,
			},
			"showThresholdLabels":  false,
			"showThresholdMarkers": true,
			"sizing":               "auto",
		}
	default:
		return map[string]interface{}{}
	}
}

func generatePanelFieldConfig(panelType string, panelTitle string) map[string]interface{} {
	switch panelType {
	case "stat":
		if panelTitle == "HTTP write requests count" || panelTitle == "HTTP query requests count" || panelTitle == "Bucket cardinality" || panelTitle == "BoltDb Writes" {
			return map[string]interface{}{
				"defaults": map[string]interface{}{
					"mappings": []string{},
					"thresholds": map[string]interface{}{
						"mode": "absolute",
						"steps": []interface{}{
							map[string]interface{}{
								"color": "yellow",
								"value": nil,
							},
							map[string]interface{}{
								"color": "green",
								"value": 1,
							},
						},
					},
					"noValue": "0",
				},
				"overrides": []string{},
			}
		} else {
			return map[string]interface{}{
				"defaults": map[string]interface{}{
					"mappings": []string{},
					"thresholds": map[string]interface{}{
						"mode": "absolute",
						"steps": []interface{}{
							map[string]interface{}{
								"color": "green",
								"value": nil,
							},
						},
					},
					"noValue": 0,
					"unit":    "bytes",
				},
				"overrides": []string{},
			}
		}
	case "bargauge":
		return map[string]interface{}{"defaults": map[string]interface{}{
			"mappings": []string{},
			"thresholds": map[string]interface{}{
				"mode": "absolute",
				"steps": []interface{}{
					map[string]interface{}{
						"color": "yellow",
						"value": nil,
					},
					map[string]interface{}{
						"color": "green",
						"value": 1,
					},
				},
			},
			"color": map[string]interface{}{
				"mode": "thresholds",
			},
		},
			"overrides": []interface{}{
				map[string]interface{}{
					"matcher": map[string]interface{}{
						"id":      "byName",
						"options": "sum",
					},
					"properties": []interface{}{
						map[string]interface{}{
							"id": "thresholds",
							"value": map[string]interface{}{
								"mode": "absolute",
								"steps": []interface{}{
									map[string]interface{}{
										"color": "green",
										"value": nil,
									},
									map[string]interface{}{
										"color": "yellow",
										"value": 1,
									},
									map[string]interface{}{
										"color": "red",
										"value": 50,
									},
								},
							},
						},
					},
				},
			},
		}
	case "gauge":
		return map[string]interface{}{"defaults": map[string]interface{}{
			"mappings": []string{},
			"thresholds": map[string]interface{}{
				"mode": "percentage",
				"steps": []interface{}{
					map[string]interface{}{
						"color": "green",
						"value": nil,
					},
					map[string]interface{}{
						"color": "yellow",
						"value": 60,
					},
					map[string]interface{}{
						"color": "red",
						"value": 80,
					},
				},
			},
			"color": map[string]interface{}{
				"mode": "thresholds",
			},
			"unit": "percent",
			"min":  0,
			"max":  100,
		},
			"overrides": []interface{}{},
		}
	default:
		return map[string]interface{}{}
	}
}

func generatePanels(datasourceName string) []interface{} {

	panelFields := []panelField{
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 0}, "Query execution duration in seconds", "bargauge", "A", "Sum", "qc_executing_duration_seconds_0.025"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 0}, "Memory utilization", "gauge", "B", "Average", "MemoryUtilization"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 8}, "CPU utilization", "gauge", "C", "Average", "CPUUtilization"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 8}, "Disk utilization", "gauge", "D", "Average", "DiskUtilization"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 16}, "Total available memory from system", "stat", "E", "Maximum", "go_memstats_sys_bytes_gauge"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 16}, "Bucket cardinality", "stat", "F", "Maximum", "storage_bucket_series_num_gauge"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 24}, "Memory cache usage", "stat", "G", "Maximum", "go_memstats_mcache_inuse_bytes_gauge"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 24}, "BoltDb writes", "stat", "H", "Maximum", "boltdb_writes_total_counter"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 32}, "Allocated memory", "stat", "I", "Maximum", "go_memstats_alloc_bytes_gauge"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 12, "y": 32}, "HTTP write requests count", "stat", "J", "Maximum", "http_write_request_count_counter"},
		{map[string]interface{}{"h": 8, "w": 12, "x": 0, "y": 40}, "HTTP query requests count", "stat", "K", "Maximum", "http_query_request_count_counter"},
	}

	var panelConfig []interface{}
	for _, panel := range panelFields {
		panelConfig = append(
			panelConfig,
			map[string]interface{}{
				"gridPos": panel.gridPosition,
				"targets": []interface{}{
					map[string]interface{}{
						"datasource": datasourceName,
						"region":     "default",
						"logGroups":  []interface{}{},
						"queryMode":  "Metrics",
						"namespace":  "AWS/Timestream/InfluxDB",
						"metricName": panel.metricName,
						"expression": "",
						"dimensions": map[string]interface{}{
							"DbInstanceName": "$instanceName",
						},
						"statistic":        panel.statisticType,
						"period":           "",
						"metricQueryType":  0,
						"metricEditorMode": 0,
						"sqlExpression":    "",
						"matchExact":       true,
						"refId":            panel.refId,
						"hide":             false,
						"label":            "",
					},
				},
				"title":       panel.title,
				"type":        panel.panelType,
				"options":     generatePanelOptions(panel.panelType),
				"fieldConfig": generatePanelFieldConfig(panel.panelType, panel.title),
			},
		)
	}

	return panelConfig
}

func generateDashboard(datasourceName string, dashboardName string, dbInstanceNames string) map[string]interface{} {

	currentTemplateOptions := map[string]interface{}{}
	templateOptions := []interface{}{}
	firstOption := true
	for _, dbInstanceName := range strings.Split(dbInstanceNames, ",") {

		templateOptions = append(
			templateOptions, map[string]interface{}{
				"selected": firstOption,
				"text":     dbInstanceName,
				"value":    dbInstanceName,
			},
		)

		if firstOption {
			currentTemplateOptions["current"] = map[string]interface{}{
				"selected": false,
				"text":     dbInstanceName,
				"value":    dbInstanceName,
			}
			firstOption = false
		}
	}

	dashboardConfig := map[string]interface{}{
		"overwrite": true,
		"folder":    0,
		"dashboard": map[string]interface{}{
			"panels": generatePanels(datasourceName),
			"title":  dashboardName,
			"templating": map[string]interface{}{
				"list": []interface{}{
					map[string]interface{}{
						"current":     currentTemplateOptions["current"],
						"hide":        0,
						"includeAll":  false,
						"label":       "",
						"multi":       false,
						"name":        "instanceName", // Variable name used in Grafana
						"options":     templateOptions,
						"query":       dbInstanceNames,
						"queryValue":  "",
						"skipUrlSync": false,
						"type":        "custom",
					},
				},
			},
		},
		"time": map[string]interface{}{
			"from": "now-15m",
			"to":   "now",
		},
	}

	return dashboardConfig
}
