// Command serde-struct-with-times checkpoints a struct whose fields are
// [time.Time] values. It corresponds to the reference example
// serde/class-with-dates.
//
// In the reference SDK, JSON turns a Date into a string, and a dedicated
// serdes rehydrates the configured fields into Date objects so their methods
// work again after replay. Go needs no such step: [time.Time] implements
// [encoding/json.Marshaler] and [encoding/json.Unmarshaler], so the default
// serdes writes each field as an RFC 3339 string and reads it back as a
// [time.Time], methods included. A nil *time.Time round-trips as JSON null
// and stays nil.
//
// The second half of the example replaces the default with a serdes that
// changes the wire format to Unix milliseconds. The handler sees the same
// values either way; only the checkpoint payload differs, which the handler
// test inspects.
package main

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aws/aws-durable-execution-sdk-go/durable"
)

// Article is a record whose timestamps must stay usable time.Time values
// across replay so that date arithmetic keeps working.
type Article struct {
	Title      string     `json:"title"`
	CreatedAt  time.Time  `json:"createdAt"`
	Metadata   Metadata   `json:"metadata"`
	ArchivedAt *time.Time `json:"archivedAt"` // nil until the article is archived
}

// Metadata nests a further timestamp one level down.
type Metadata struct {
	PublishedAt time.Time `json:"publishedAt"`
}

// IsPublished calls a time.Time method, so it works only if PublishedAt
// came back from the checkpoint as a time.Time.
func (a Article) IsPublished() bool { return !a.Metadata.PublishedAt.IsZero() }

// Age returns how long after CreatedAt the instant now is.
func (a Article) Age(now time.Time) time.Duration { return now.Sub(a.CreatedAt) }

// Fixed instants keep the example deterministic across invocations.
var (
	createdAt   = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)
	publishedAt = time.Date(2020, time.January, 2, 0, 0, 0, 0, time.UTC)
	inspectedAt = time.Date(2020, time.January, 3, 0, 0, 0, 0, time.UTC)
)

// newArticle builds the article both steps return.
func newArticle(title string) Article {
	return Article{
		Title:     title,
		CreatedAt: createdAt,
		Metadata:  Metadata{PublishedAt: publishedAt},
		// ArchivedAt intentionally left nil.
	}
}

// articleWire is the checkpoint form written by epochSerdes: every
// timestamp is a Unix millisecond count instead of an RFC 3339 string.
type articleWire struct {
	Title         string `json:"title"`
	CreatedAtMs   int64  `json:"createdAtMs"`
	PublishedAtMs int64  `json:"publishedAtMs"`
	ArchivedAtMs  *int64 `json:"archivedAtMs"`
}

// epochSerdes stores an Article's timestamps as Unix milliseconds. It shows
// that a custom serdes changes the checkpoint payload without changing the
// value the handler works with.
var epochSerdes = durable.SerdesOf(
	func(_ context.Context, _ durable.SerdesContext, a Article) ([]byte, error) {
		w := articleWire{
			Title:         a.Title,
			CreatedAtMs:   a.CreatedAt.UnixMilli(),
			PublishedAtMs: a.Metadata.PublishedAt.UnixMilli(),
		}
		if a.ArchivedAt != nil {
			ms := a.ArchivedAt.UnixMilli()
			w.ArchivedAtMs = &ms
		}
		return json.Marshal(w)
	},
	func(_ context.Context, _ durable.SerdesContext, data []byte) (Article, error) {
		var w articleWire
		if err := json.Unmarshal(data, &w); err != nil {
			return Article{}, err
		}
		a := Article{
			Title:     w.Title,
			CreatedAt: time.UnixMilli(w.CreatedAtMs).UTC(),
			Metadata:  Metadata{PublishedAt: time.UnixMilli(w.PublishedAtMs).UTC()},
		}
		if w.ArchivedAtMs != nil {
			t := time.UnixMilli(*w.ArchivedAtMs).UTC()
			a.ArchivedAt = &t
		}
		return a, nil
	},
)

// inspection is what the handler observes about an article after replay.
type inspection struct {
	Title           string `json:"title"`
	CreatedAt       string `json:"createdAt"`
	PublishedAt     string `json:"publishedAt"`
	IsPublished     bool   `json:"isPublished"`
	AgeHours        int    `json:"ageHours"`
	ArchivedAtIsNil bool   `json:"archivedAtIsNil"`
	EqualsOriginal  bool   `json:"equalsOriginal"`
}

// inspect reports the article's fields in a form that is stable across
// runs. Equality uses time.Time.Equal, which compares instants regardless
// of the Location a decoded value carries.
func inspect(a Article) inspection {
	want := newArticle(a.Title)
	return inspection{
		Title:           a.Title,
		CreatedAt:       a.CreatedAt.UTC().Format(time.RFC3339),
		PublishedAt:     a.Metadata.PublishedAt.UTC().Format(time.RFC3339),
		IsPublished:     a.IsPublished(),
		AgeHours:        int(a.Age(inspectedAt).Hours()),
		ArchivedAtIsNil: a.ArchivedAt == nil,
		EqualsOriginal: a.Title == want.Title &&
			a.CreatedAt.Equal(want.CreatedAt) &&
			a.Metadata.PublishedAt.Equal(want.Metadata.PublishedAt) &&
			(a.ArchivedAt == nil) == (want.ArchivedAt == nil),
	}
}

type event struct {
	Title string `json:"title"`
}

type output struct {
	DefaultSerdes inspection `json:"defaultSerdes"`
	EpochSerdes   inspection `json:"epochSerdes"`
}

func handler(ctx durable.Context, ev event) (output, error) {
	title := ev.Title
	if title == "" {
		title = "Durable Functions 101"
	}

	// Default serdes: the timestamps are written as RFC 3339 strings.
	byDefault, err := durable.Step(ctx, "create-article", func(_ durable.StepContext) (Article, error) {
		return newArticle(title), nil
	})
	if err != nil {
		return output{}, err
	}

	// Custom serdes: the same article is written as Unix milliseconds.
	byEpoch, err := durable.Step(ctx, "create-article-epoch", func(_ durable.StepContext) (Article, error) {
		return newArticle(title), nil
	}, durable.WithStepSerdes(epochSerdes))
	if err != nil {
		return output{}, err
	}

	// The wait forces a replay, so both articles below are the values
	// decoded from their checkpoints, not the ones the steps returned.
	if err := durable.Wait(ctx, "replay", 1*time.Second); err != nil {
		return output{}, err
	}

	return durable.Step(ctx, "inspect-articles", func(_ durable.StepContext) (output, error) {
		return output{DefaultSerdes: inspect(byDefault), EpochSerdes: inspect(byEpoch)}, nil
	})
}

func main() { durable.Start(handler) }
