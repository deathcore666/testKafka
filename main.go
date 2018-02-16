package main

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
	"encoding/json"
	"bufio"
	"os/signal"
	"time"
)

type Country struct {
	Name string   `json:"name"`
	Code []string `json:"code"`
	Gdp  float64  `json:"GdpPerCapita"`
}

var codes []Country
var phones = map[string]int{
	"iPhone4":  1,
	"iPhone5":  3,
	"iPhone6":  5,
	"iPhone7":  6,
	"iPhone8":  8,
	"iPhoneX":  9,
	"Galaxys5": 2,
	"Galaxys6": 3,
	"Galaxys7": 6,
	"Galaxys8": 9,
}

func main() {
	start := time.Now()
	timer := time.NewTimer(time.Second * 10000)

	dataStream := make(chan []byte)
	go readFile(dataStream)

	for line := range dataStream {
		var code Country
		err := json.Unmarshal(line, &code)
		if err != nil {
			log.Println(err)
		}
		codes = append(codes, code)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	data := make(chan []byte)
	errors := make(chan string)

	messages := 0

	go consume(data, errors)
Loop:
	for {
		select {
		case msg := <-data:
			go digest(msg)
			messages++
			if (messages > 50000) {
				break Loop
			}
		case err := <-errors:
			log.Println(err)
		case <-timer.C:
			log.Println("timer")
			break Loop
		case <-signals:
			break Loop
		}
	}

	t := time.Now()
	elapsed := t.Sub(start)
	log.Println("Time elapsed: ", elapsed)
	log.Println("Digested messages count: ", messages)
}

func digest(msg []byte) {
	data := make(map[string]interface{})

	err := json.Unmarshal(msg, &data)
	if err != nil {
		log.Println("Digest", err)
	}

	dest := data["dest_number"].(string)
	dest = dest[1:4]

	phone := data["phone_model"]

	for _, code := range codes {
		for i := range code.Code {
			if dest == code.Code[i] {
				log.Println(code.Name)
			}
		}
	}

	for k, v := range phones {
		if phone == k {
			log.Println(k, v)
		}
	}
	//log.Println(data["dest_number"])
	//log.Println(data["phone_model"])
}

func readFile(dataStream chan []byte) {
	file, err := os.Open("countryCodes.json")
	if err != nil {
		log.Println(err)
	}

	defer file.Close()

	fileScanner := bufio.NewScanner(file)
	for fileScanner.Scan() {
		dataStream <- []byte(fileScanner.Text())
	}
	close(dataStream)
}

func consume(data chan []byte, errors chan string) {
	brokers := []string{"localhost:9092"}
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		panic(err)
	}

	partitionList, err := consumer.Partitions("sanjar3")

	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition("sanjar3", partition, sarama.OffsetOldest)

		go func() {
			for {
				select {
				case msg := <-pc.Messages():
					data <- msg.Value
				case err := <-pc.Errors():
					errors <- err.Error()
				}
			}
		}()
	}
}
