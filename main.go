package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/segmentio/kafka-go"
)

// ===== Structs =====
type Article struct {
	Title  string `json:"title"`
	URL    string `json:"url"`
	Source string `json:"source"`
}

// ===== Rate Limiter =====
type RateLimiter struct {
	mu       sync.Mutex
	requests map[string]int
	limit    int
	window   time.Duration
}

func NewRateLimiter(limit int, window time.Duration) *RateLimiter {
	return &RateLimiter{
		requests: make(map[string]int),
		limit:    limit,
		window:   window,
	}
}

func (rl *RateLimiter) Limit(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ip := strings.Split(r.RemoteAddr, ":")[0]

		rl.mu.Lock()
		defer rl.mu.Unlock()

		if rl.requests[ip] >= rl.limit {
			http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
			return
		}

		rl.requests[ip]++
		go func() {
			time.Sleep(rl.window)
			rl.mu.Lock()
			rl.requests[ip]--
			rl.mu.Unlock()
		}()

		next.ServeHTTP(w, r)
	})
}

// ===== Scraper =====
func scrapeSource(url string, wg *sync.WaitGroup, ch chan<- Article) {
	defer wg.Done()

	resp, err := http.Get(url)
	if err != nil {
		log.Println("Failed to fetch:", url, err)
		return
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		log.Println("Document parse error:", err)
		return
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		title := strings.TrimSpace(s.Text())
		href, exists := s.Attr("href")

		if exists && title != "" && strings.HasPrefix(href, "https://dantri.com.vn") {
			ch <- Article{
				Title:  title,
				URL:    href,
				Source: url,
			}
		}
	})
}

func ScrapeNews(sources []string) []Article {
	var wg sync.WaitGroup
	ch := make(chan Article, 100)

	for _, src := range sources {
		wg.Add(1)
		go scrapeSource(src, &wg, ch)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	var results []Article
	for article := range ch {
		results = append(results, article)
	}

	return results
}

// ===== Kafka Utils =====
func getKafkaBrokers() []string {
	brokers := os.Getenv("KAFKA_BROKER")
	if brokers == "" {
		return []string{"localhost:9092"} // fallback
	}
	return strings.Split(brokers, ",")
}

func publishToKafka(topic string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: getKafkaBrokers(),
		Topic:   topic,
	})
	defer writer.Close()

	return writer.WriteMessages(context.Background(), kafka.Message{Value: data})
}

// ===== Kafka Consumer =====
func consumeFromKafka(topic string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: getKafkaBrokers(),
		Topic:   topic,
		GroupID: "go-news-group",
	})
	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println("Consumer error:", err)
			continue
		}
		log.Printf("Consumed from [%s]: %s\n", topic, string(msg.Value))
	}
}

// ===== HTTP Handlers =====
func GetArticlesHandler(w http.ResponseWriter, r *http.Request) {
	sources := []string{"https://vnexpress.net", "https://dantri.com.vn"}
	articles := ScrapeNews(sources)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(articles)
}

func PublishArticleHandler(w http.ResponseWriter, r *http.Request) {
	var payload struct {
		Article Article `json:"article"`
	}

	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil || payload.Article.Title == "" {
		http.Error(w, "Invalid article data", http.StatusBadRequest)
		return
	}

	if err := publishToKafka("news_topic", payload.Article); err != nil {
		http.Error(w, fmt.Sprintf("Kafka error: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	fmt.Fprintln(w, "Article published to Kafka")
}

func main() {
	rateLimiter := NewRateLimiter(100, time.Minute)

	go consumeFromKafka("news_topic")

	mux := http.NewServeMux()
	mux.HandleFunc("/articles", GetArticlesHandler)
	mux.HandleFunc("/publish", PublishArticleHandler)

	// Serve static frontend
	mux.Handle("/public/", http.StripPrefix("/public/", http.FileServer(http.Dir("./static"))))

	handler := rateLimiter.Limit(mux)

	log.Println("✅ Server started at http://localhost:8086")
	log.Fatal(http.ListenAndServe(":8086", handler))
}
