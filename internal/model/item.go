package model

import (
	"fmt"
)

type Item struct {
	ID               int    `json:"id"`
	Title            string `json:"title"`
	Slug             string `json:"slug"` // Make sure to include Slug for URL generation
	Description      string `json:"description"`
	Price            string `json:"price"`
	CreatedDT        string `json:"created_dt"`
	OwnerAdvertCount int    `json:"owner_advert_count"`
	NegotiablePrice  bool   `json:"negotiable_price"`
	Rubric           int    `json:"rubric"`
	City             int    `json:"city"`
	UserID           int    `json:"user_id"`
	Currency         string `json:"currency"`
	RaiseDT          string `json:"raise_dt"`
}

// GenerateURL constructs the URL based on the item's ID and Slug
func (i *Item) GenerateURL() string {
	return fmt.Sprintf("https://www.bazaraki.com/adv/%d_%s/", i.ID, i.Slug)
}
