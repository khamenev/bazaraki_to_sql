package api

import (
	"bazaraki_to_sql/internal/model"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

const (
	//baseURL        = "https://www.bazaraki.com/api/items/?rubric=681&city=12"
	initialBackoff = 3 * time.Second
	maxRetries     = 100
)

// https://www.bazaraki.com/api/items/?rubric=681&city=12 houses
// https://www.bazaraki.com/api/items/?rubric=3529 app

type ApiResponse struct {
	Results []model.Item `json:"results"`
	Next    string       `json:"next"`
	Count   int          `json:"count"`
}

func FetchPage(url string, page int) ([]model.Item, string, error) {
	attempt := 0
	backoff := initialBackoff

	for attempt < maxRetries {
		resp, err := http.Get(fmt.Sprintf("%s&page=%d", url, page))
		if err != nil {
			log.Printf("Error fetching page %d: %v", page, err)
			time.Sleep(backoff)
			backoff *= 2
			attempt++
			continue
		}

		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var apiResponse ApiResponse
			body, _ := ioutil.ReadAll(resp.Body)
			json.Unmarshal(body, &apiResponse)
			var pages = apiResponse.Count / 10
			log.Printf("Fetched page: %d from %v", page, pages)
			return apiResponse.Results, apiResponse.Next, nil
		} else if resp.StatusCode == http.StatusTooManyRequests || (resp.StatusCode >= 500 && resp.StatusCode < 600) || resp.StatusCode == 403 {
			// Handle retryable status codes
			log.Printf("Retryable error, status code: %d", resp.StatusCode)
			time.Sleep(backoff)
			backoff *= 2
			attempt++
			continue
		} else {
			// For non-retryable errors, break the loop and return the error
			return nil, "", fmt.Errorf("failed with status code: %d", resp.StatusCode)
		}
	}

	return nil, "", fmt.Errorf("retries exceeded for page %d", page)
}
